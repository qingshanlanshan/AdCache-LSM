#include <torch/torch.h>

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <semaphore>

#define LOGGING 1
#define LOAD_MODEL 1
#define SAVE_MODEL 0
#define ONLINE_LEARNING 0

class SafeBinarySemaphore {
 public:
  SafeBinarySemaphore() : sem(0), signaled(false) {}

  // Only call release if not already signaled.
  void release() {
    bool expected = false;
    // Attempt to set 'signaled' to true; if it was already true, do nothing.
    if (signaled.compare_exchange_strong(expected, true)) {
      sem.release();
    }
  }

  // Acquire the semaphore and reset the signaled flag.
  void acquire() {
    sem.acquire();
    signaled.store(false);
  }

 private:
  std::binary_semaphore sem;
  std::atomic<bool> signaled;
};

struct ActorImpl : torch::nn::Module {
  torch::nn::Linear fc1{nullptr}, fc2{nullptr}, fc3{nullptr};

  ActorImpl(int state_dim, int hidden_dim, int action_dim) {
    fc1 = register_module("fc1", torch::nn::Linear(state_dim, hidden_dim));
    fc2 = register_module("fc2", torch::nn::Linear(hidden_dim, hidden_dim));
    fc3 = register_module("fc4", torch::nn::Linear(hidden_dim, action_dim));
  }

  torch::Tensor forward(torch::Tensor state) {
    auto x = torch::relu(fc1->forward(state));
    x = torch::relu(fc2->forward(x));
    // Use sigmoid to produce outputs in [0,1].
    x = torch::sigmoid(fc3->forward(x));
    return x;
  }
};
TORCH_MODULE(Actor);

// Critic network: estimates Q-values for a given state and action.
struct CriticImpl : torch::nn::Module {
  torch::nn::Linear fc1{nullptr}, fc2{nullptr}, fc3{nullptr};

  CriticImpl(int state_dim, int action_dim, int hidden_dim) {
    // Combine state and action as input.
    fc1 = register_module(
        "fc1", torch::nn::Linear(state_dim + action_dim, hidden_dim));
    fc2 = register_module("fc2", torch::nn::Linear(hidden_dim, hidden_dim));
    fc3 = register_module("fc3", torch::nn::Linear(hidden_dim, 1));
  }

  torch::Tensor forward(torch::Tensor state, torch::Tensor action) {
    auto x = torch::cat({state, action}, 1);
    x = torch::relu(fc1->forward(x));
    x = torch::relu(fc2->forward(x));
    x = fc3->forward(x);
    return x;
  }
};
TORCH_MODULE(Critic);

const int RANGE_COUNT = 1;
const int WORKLOAD_DIM = RANGE_COUNT + 4;  // Workload input dimension.
const int CACHE_PARAM_DIM =
    RANGE_COUNT * 2 + 2;  // Current cache parameters dimension.
// const int STATE_DIM = WORKLOAD_DIM + CACHE_PARAM_DIM;
const int STATE_DIM = WORKLOAD_DIM;
const int ACTION_DIM =
    CACHE_PARAM_DIM;  // We'll adjust the cache parameters continuously.
const int HIDDEN_DIM = 640;
const double ACTOR_LR = 1e-4;
const double CRITIC_LR = 1e-3;
const double GAMMA = 0.99;
const int NUM_STEPS = 1;
int n_updates = 0;

// updated by sub thread
// 0～-3: range lookup param
// -2: point lookup param
// -1: block cache size
std::vector<float> cache_params_vector(CACHE_PARAM_DIM, 1.0);

// updated by main thread
std::vector<float> workload_vector(WORKLOAD_DIM, 0.0);
std::vector<float> workload_vector_prev(WORKLOAD_DIM, 0.0);

float cur_hitrate = 0.5;
std::shared_mutex vector_mutex;
// std::binary_semaphore sem(0);
SafeBinarySemaphore sem;
std::atomic<bool> exit_train_worker{false};

Actor actor(STATE_DIM, HIDDEN_DIM, ACTION_DIM);
Critic critic(STATE_DIM, ACTION_DIM, HIDDEN_DIM);

torch::optim::Adam actor_optimizer(actor->parameters(),
                                   torch::optim::AdamOptions(ACTOR_LR));
torch::optim::Adam critic_optimizer(critic->parameters(),
                                    torch::optim::AdamOptions(CRITIC_LR));

double smoothed_hitrate = cur_hitrate;
auto prev_state = torch::zeros({1, STATE_DIM}, torch::kFloat32);

std::string actor_path = "/home/jiarui/CacheLSM/models/actor.pt";
std::string critic_path = "/home/jiarui/CacheLSM/models/critic.pt";

bool is_workload_changed() {
  for (int i = 0; i < WORKLOAD_DIM; i++) {
    if (abs(workload_vector[i] - workload_vector_prev[i]) > 0.05) {
      return true;
    }
  }
  return false;
}

// ------------------------
// Main Training Loop
// ------------------------
void update() {
  n_updates++;
  // calculate reward
  double new_smoothed_hitrate = cur_hitrate * 0.1 + smoothed_hitrate * 0.9;
  double reward = (new_smoothed_hitrate - smoothed_hitrate) / smoothed_hitrate;
  smoothed_hitrate = new_smoothed_hitrate;
  if (LOGGING) {
    std::cout << "cur_hitrate: " << cur_hitrate << std::endl;
    std::cout << "smoothed_hitrate: " << smoothed_hitrate << std::endl;
    std::cout << "Reward: " << reward << std::endl;
  }
  // unlikely
  while (workload_vector.size() < WORKLOAD_DIM) workload_vector.push_back(0.0);
  while (workload_vector.size() > WORKLOAD_DIM) workload_vector.pop_back();
  workload_vector_prev = workload_vector;

  auto workload = torch::tensor(workload_vector).unsqueeze(0);
  auto current_cache_params = torch::tensor(cache_params_vector).unsqueeze(0);
  // auto state = torch::cat({workload, current_cache_params}, 1);
  auto state = workload;

  auto action = actor->forward(state);
  if (ONLINE_LEARNING) {
    auto q_value = critic->forward(state, action).detach();
    auto target_tensor =
        torch::tensor({{reward}}, torch::kFloat32) + GAMMA * q_value;

    auto q = critic->forward(prev_state, current_cache_params);
    auto critic_loss = torch::mse_loss(q, target_tensor);

    critic_optimizer.zero_grad();
    critic_loss.backward();
    torch::nn::utils::clip_grad_norm_(critic->parameters(), 1.0);
    critic_optimizer.step();

    auto actor_action = actor->forward(prev_state);
    auto actor_loss = -critic->forward(prev_state, actor_action).mean();

    actor_optimizer.zero_grad();
    actor_loss.backward();
    torch::nn::utils::clip_grad_norm_(actor->parameters(), 1.0);
    actor_optimizer.step();
  }
  // std::cout<< state << std::endl;
  action = actor->forward(state);
  // std::cout<< action << std::endl;
  // check nan
  if (action.isnan().any().item<bool>()) {
    std::cout << "NaN detected in action!" << std::endl;
  }
  else
    current_cache_params = action.detach();
  prev_state = state.detach();
  std::vector<float> vec(
      current_cache_params.data_ptr<float>(),
      current_cache_params.data_ptr<float>() + current_cache_params.numel());
  {
    std::shared_lock<std::shared_mutex> lock(vector_mutex);
    cache_params_vector = vec;
    assert(cache_params_vector.size() == CACHE_PARAM_DIM);
  }
}

void train_worker_function() {
  if (LOAD_MODEL) {
    if (!std::filesystem::exists(actor_path) ||
        !std::filesystem::exists(critic_path)) {
      std::cout << "No model found, training from scratch." << std::endl;
    } else {
      torch::load(actor, actor_path);
      torch::load(critic, critic_path);
      std::cout << "Model loaded." << std::endl;
    }
  }
  // load models
  while (!exit_train_worker.load()) {
    // Wait until a signal is received.
    sem.acquire();
    if (exit_train_worker.load()) break;
    update();
    // LOGGING
    if (LOGGING) {
      std::cout << "--------------------------------" << std::endl;
      std::cout << "update: " << n_updates << std::endl;
      std::cout << "workload: ";
      for (int i = 0; i < WORKLOAD_DIM; i++) {
        std::cout << workload_vector[i] << " ";
      }
      std::cout << std::endl;
      std::cout << "cache params: ";
      for (int i = 0; i < CACHE_PARAM_DIM; i++) {
        std::cout << cache_params_vector[i] << " ";
      }
      std::cout << std::endl;
    }
  }
  if (SAVE_MODEL) {
    torch::save(actor, actor_path);
    torch::save(critic, critic_path);
    std::cout << "Model saved." << std::endl;
  }
}