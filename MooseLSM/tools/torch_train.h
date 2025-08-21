#include <torch/torch.h>

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <semaphore>

#include "rocksdb/advanced_cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "tools/sharded_cache/Utils.h"
#include "tools/sharded_cache/ShardedCacheBase.h"

#define LOGGING 1
#define LOAD_MODEL 1
#define SAVE_MODEL 0
#define ONLINE_LEARNING 0

// updated by main thread
std::vector<float> workload_vector(WORKLOAD_DIM, 0.0);
std::vector<float> workload_vector_prev(WORKLOAD_DIM, 0.0);

float cur_hitrate = 0.5;

class SafeBinarySemaphore {
public:
  SafeBinarySemaphore() : sem_(0), signaled_(false) {}

  // Non-blocking trigger: coalesces multiple releases into one.
  // Returns true iff this call actually signaled the worker.
  bool trigger() {
    bool expected = false;
    if (signaled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      // It's critical we only call release() when transitioning false->true,
      // because std::binary_semaphore is a counting_semaphore<1>; overflowing is UB.
      sem_.release();
      return true;
    }
    return false; // already signaled or worker still running
  }

  // Worker side: wait until triggered.
  void wait() {
    sem_.acquire();
    // Keep signaled_ == true while work is in progress to coalesce further triggers.
    // (So calls to trigger() during work won't release again.)
  }

  // Worker marks completion; allows a *new* trigger to arm another run.
  void done() {
    signaled_.store(false, std::memory_order_release);
  }

  // Optional: non-blocking check (worker poll style).
  bool try_wait() {
    if (sem_.try_acquire()) return true;
    return false;
  }

  // Optional: report whether a run is queued or in-progress.
  bool is_signaled() const {
    return signaled_.load(std::memory_order_acquire);
  }

private:
  std::binary_semaphore sem_;
  std::atomic<bool>     signaled_;
};

void sync_model_cache(rocksdb::DB* db = nullptr,
                      size_t max_cache_size = 1e8,
                      size_t kvsize = 1024,
                      std::optional<ShardedCacheBase> cache = std::nullopt,
                      std::ostream* out = &std::cout) {
  if (!db || !cache || cache->name != "adcache") return;

  learning_stats.warmup_done = cache->warmup_done();
  if (learning_stats.warmup_done) {
// sync cache capacity
#if 1
    float cache_ratio = 0;
    {
      std::shared_lock<std::shared_mutex> lock(vector_mutex);
      cache_ratio = cache_params_vector[params_put_idx()];
    }
     
    // gradually change the cache size to avoid too much eviction
    cache_ratio = cache_ratio * 0.2 + 0.8 * cache->get_capacity() / max_cache_size;
    std::clamp(cache_ratio, 0.01f, 0.99f);
    if (learning_stats.n_scan == 0) {
      cache_ratio = (float)cache->get_capacity() / max_cache_size;
      cache_ratio = std::max((float)0.1, cache_ratio);
      if (cache->warmup_done())
        cache_ratio = std::min(1.0, cache_ratio + 0.1);
    } else {
      cache_ratio *= 0.94;
    }

    // cache_ratio = 0 or 1 may cause unexpected behaviour
    cache_ratio = std::clamp(cache_ratio, 0.01f, 0.99f);

    // update cache
    if (ENABLE_ADAPTIVE_PARTITIONING) {
      float cur_blockcache_ratio = 1 - cache_ratio;
      cache->set_capacity((size_t)(cache_ratio * max_cache_size));
      auto blockcache = db->GetOptions().extern_options->block_cache;
      size_t size = cur_blockcache_ratio * max_cache_size * kvsize;
      blockcache->SetCapacity(size);
      std::cout << "blockcache ratio: " << cur_blockcache_ratio << std::endl;
    }
    // update model
    {
      std::unique_lock<std::shared_mutex> lock(vector_mutex);
      cache_params_vector[params_put_idx()] = cache_ratio;
      workload_vector = learning_stats.ToVector();
      cur_hitrate = std::accumulate(hit_rates.begin(), hit_rates.end(), 0.0) / hit_rates.size();
      hit_rates.clear();
    }
    // *out << "blockcache ratio: " << cur_blockcache_ratio << std::endl;
#endif
  }
  // reset
  learning_stats.reset();
}

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
  double reward_clipped = std::clamp(reward, -1.0, 1.0);
  ACTOR_LR *= (1.0 - 0.1 * reward_clipped);
  ACTOR_LR = std::clamp(ACTOR_LR, 1e-6, 1e-2);
  for (auto& group : actor_optimizer.param_groups()) {
    group.options().set_lr(ACTOR_LR);
  }

  if (LOGGING) {
    std::cout << "cur_hitrate: " << cur_hitrate << std::endl;
    std::cout << "smoothed_hitrate: " << smoothed_hitrate << std::endl;
    std::cout << "Reward: " << reward << std::endl;
    std::cout << "Actor LR: " << ACTOR_LR << std::endl;
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
    std::unique_lock<std::shared_mutex> lock(vector_mutex);
    cache_params_vector = vec;
    assert(cache_params_vector.size() == CACHE_PARAM_DIM);
  }
}

void train_worker_function(rocksdb::DB* db, size_t max_cache_size, size_t kv_size, std::optional<ShardedCacheBase>& cache, std::ostream* out = &std::cout) {
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
    // sem.acquire();
    sem.wait();
    if (exit_train_worker.load()) break;
    sync_model_cache(db, max_cache_size, kv_size, cache, out);
    update();
    sem.done();
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