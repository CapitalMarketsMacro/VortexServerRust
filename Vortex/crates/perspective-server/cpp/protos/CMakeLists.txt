cmake_minimum_required(VERSION 3.18.2)
project(perspective-protos)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PSP_PROTO_PATH "Path to the perspective proto directory" "")

if(NOT DEFINED psp_build_message)
    function(psp_build_message message)
        set(BUILD_MESSAGE "${BUILD_MESSAGE}\n${message}")
    endfunction()
endif()

# Include the FindProtobuf.cmake module
list(APPEND CMAKE_MODULE_PATH "../../cmake/modules")

if(NOT DEFINED PSP_CMAKE_MODULE_PATH)
    set(PSP_CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../cmake")
    message(STATUS "PSP_CMAKE_MODULE_PATH not defined, using ${PSP_CMAKE_MODULE_PATH}")
endif()

# Detect Conan toolchain
if(DEFINED CMAKE_TOOLCHAIN_FILE AND CMAKE_TOOLCHAIN_FILE MATCHES "conan_toolchain")
    set(PSP_USE_CONAN TRUE)
else()
    set(PSP_USE_CONAN FALSE)
endif()

if(PSP_USE_CONAN)
    # === Conan path ===
    find_package(Protoc REQUIRED)

    # Load Conan protobuf data to get include/lib paths.
    # The protobuf::protobuf target may not be available yet since this
    # subdirectory is added early. Load the data files directly.
    cmake_path(GET CMAKE_TOOLCHAIN_FILE PARENT_PATH _conan_gen)
    # Remove UNC prefix //?/ that breaks file(GLOB) on Windows
    string(REGEX REPLACE "^//\\?/" "" _conan_gen "${_conan_gen}")

    file(GLOB _pb_data "${_conan_gen}/protobuf-*-data.cmake")
    foreach(_f ${_pb_data})
        include(${_f})
    endforeach()
    # Also load abseil data — protobuf headers depend on abseil
    file(GLOB _absl_data "${_conan_gen}/abseil-*-data.cmake" "${_conan_gen}/absl-*-data.cmake")
    foreach(_f ${_absl_data})
        include(${_f})
    endforeach()
    foreach(_f ${_pb_data})
        include(${_f})
    endforeach()

    set(PROTO_FILES "${PSP_PROTO_PATH}/perspective.proto")
    protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
    add_library(protos STATIC ${PROTO_SRCS} ${PROTO_HDRS})
    target_include_directories(protos PRIVATE
        ${protobuf_INCLUDE_DIRS_RELEASE}
        ${abseil_INCLUDE_DIRS_RELEASE}
    )
    target_link_directories(protos PRIVATE ${protobuf_LIB_DIRS_RELEASE})
    target_link_libraries(protos ${protobuf_protobuf_libprotobuf_LIBS_RELEASE})
    target_include_directories(protos PUBLIC ${CMAKE_BINARY_DIR}/protos-build)
else()
    # === ExternalProject path ===
    find_package(Protoc REQUIRED)
    find_package(InstallDependency REQUIRED)

    psp_build_dep("protobuf" "${PSP_CMAKE_MODULE_PATH}/protobuf.txt.in")

    set(PROTO_FILES "${PSP_PROTO_PATH}/perspective.proto")
    protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
    add_library(protos STATIC ${PROTO_SRCS} ${PROTO_HDRS})
    target_include_directories(protos PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/protobuf-src/src)
    target_include_directories(protos PUBLIC ${CMAKE_BINARY_DIR}/protos-build)
    target_link_libraries(protos protobuf::libprotobuf)
endif()
