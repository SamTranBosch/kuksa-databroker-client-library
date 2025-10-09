cmake_minimum_required(VERSION 3.10)
project(kuksa_databroker)

# Use C++17 standard.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the required packages.
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Explicitly locate the grpc_cpp_plugin (it should be installed in /usr/local/bin in our Docker build).
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin HINTS /usr/local/bin)
if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
  message(FATAL_ERROR "grpc_cpp_plugin not found. Please ensure it is installed and accessible.")
endif()

# Set the PROTO_INCLUDE directory to the parent of the "kuksa" folder.
set(PROTO_INCLUDE ${PROJECT_SOURCE_DIR}/proto)

# Define the proto file paths.
set(TYPES_PROTO ${PROTO_INCLUDE}/kuksa/val/v1/types.proto)
set(VAL_PROTO   ${PROTO_INCLUDE}/kuksa/val/v1/val.proto)

# Create output directory for generated files.
set(GENERATED_DIR ${PROJECT_SOURCE_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

# Generate protobuf code for types.proto.
add_custom_command(
  OUTPUT ${GENERATED_DIR}/kuksa/val/v1/types.pb.cc ${GENERATED_DIR}/kuksa/val/v1/types.pb.h
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
          --cpp_out=${GENERATED_DIR}
          -I ${PROTO_INCLUDE}
          ${TYPES_PROTO}
  DEPENDS ${TYPES_PROTO}
  COMMENT "Generating C++ code from ${TYPES_PROTO}"
)

# Generate protobuf code for val.proto.
add_custom_command(
  OUTPUT ${GENERATED_DIR}/kuksa/val/v1/val.pb.cc ${GENERATED_DIR}/kuksa/val/v1/val.pb.h
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
          --cpp_out=${GENERATED_DIR}
          -I ${PROTO_INCLUDE}
          ${VAL_PROTO}
  DEPENDS ${VAL_PROTO}
  COMMENT "Generating C++ code from ${VAL_PROTO}"
)

# Generate gRPC code for val.proto.
add_custom_command(
  OUTPUT ${GENERATED_DIR}/kuksa/val/v1/val.grpc.pb.cc ${GENERATED_DIR}/kuksa/val/v1/val.grpc.pb.h
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
          --grpc_out=${GENERATED_DIR}
          --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
          -I ${PROTO_INCLUDE}
          ${VAL_PROTO}
  DEPENDS ${VAL_PROTO}
  COMMENT "Generating gRPC code from ${VAL_PROTO}"
)

# List all generated source files.
set(GENERATED_SRCS
    ${GENERATED_DIR}/kuksa/val/v1/types.pb.cc
    ${GENERATED_DIR}/kuksa/val/v1/val.pb.cc
    ${GENERATED_DIR}/kuksa/val/v1/val.grpc.pb.cc
)
set(GENERATED_HDRS
    ${GENERATED_DIR}/kuksa/val/v1/types.pb.h
    ${GENERATED_DIR}/kuksa/val/v1/val.pb.h
    ${GENERATED_DIR}/kuksa/val/v1/val.grpc.pb.h
)

# Define your library source files.
# The shared library will be built from KuksaClient.cpp, vapiclient.cpp and the generated sources.
set(LIB_SOURCES
    KuksaClient.cpp
    vapiclient.cpp
    ${GENERATED_SRCS}
)

# Create the shared library target named KuksaClient.
add_library(KuksaClient SHARED ${LIB_SOURCES})

target_include_directories(KuksaClient
    PUBLIC ${PROJECT_SOURCE_DIR} ${GENERATED_DIR} ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(KuksaClient
    PUBLIC gRPC::grpc++ protobuf::libprotobuf
)

# install lib and headers
install(TARGETS KuksaClient DESTINATION lib)
install(FILES KuksaClient.hpp vapiclient.hpp DESTINATION include)


# Create an executable target for the client.
set(CLIENT_SRCS main.cpp)
add_executable(KuksaDatabrokerClient ${CLIENT_SRCS})

target_include_directories(KuksaDatabrokerClient PRIVATE ${GENERATED_DIR} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(KuksaDatabrokerClient PRIVATE KuksaClient)

# Set RPATH for the executable so it can find libKuksaClient.so at runtime.
set_target_properties(KuksaDatabrokerClient PROPERTIES
    BUILD_RPATH "${CMAKE_CURRENT_BINARY_DIR}"
    INSTALL_RPATH "$ORIGIN"
)


# Add include directories for the executable.
target_include_directories(KuksaDatabrokerClient PRIVATE ${GENERATED_DIR} ${Protobuf_INCLUDE_DIRS})

# Link the executable against the shared library.
target_link_libraries(KuksaDatabrokerClient PRIVATE KuksaClient)
