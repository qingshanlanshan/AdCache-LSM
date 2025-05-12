cmake_minimum_required(VERSION 3.18 FATAL_ERROR)

find_package(Torch REQUIRED)
message(STATUS "TORCH_LIBRARIES: ${TORCH_LIBRARIES}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER}")
set(CORE_TOOLS
  sst_dump.cc
  ldb.cc)
# find_package(Drogon CONFIG REQUIRED)
foreach(src ${CORE_TOOLS})
  get_filename_component(exename ${src} NAME_WE)
  add_executable(${exename}${ARTIFACT_SUFFIX}
    ${src})
  target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB})
  list(APPEND core_tool_deps ${exename})
endforeach()

if(WITH_TOOLS)
  set(TOOLS
    db_sanity_test.cc
    write_stress.cc
    db_repl_stress.cc
    dump/rocksdb_dump.cc
    rw_test.cc
    levelcache_test.cc
    simple_test.cc
    dump/rocksdb_undump.cc)
  foreach(src ${TOOLS})
    get_filename_component(exename ${src} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX}
      ${src})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
    list(APPEND tool_deps ${exename})
  endforeach()

  add_executable(cache_test${ARTIFACT_SUFFIX}
  cache_test.cc)
  target_link_libraries(cache_test${ARTIFACT_SUFFIX} ${TORCH_LIBRARIES} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
  list(APPEND tool_deps cache_test)

  add_executable(torch_pretrain${ARTIFACT_SUFFIX}
  torch_pretrain.cc)
  target_link_libraries(torch_pretrain${ARTIFACT_SUFFIX} ${TORCH_LIBRARIES} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
  list(APPEND tool_deps torch_pretrain)

  # TODO: Compile it separately
  # add_executable(kv_server${ARTIFACT_SUFFIX}
  #   kv_server.cc)
  # target_link_libraries(kv_server${ARTIFACT_SUFFIX} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS} Drogon::Drogon)
  add_custom_target(ldb_tests
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/ldb_tests.py
    DEPENDS ldb)
endif()
