# ----------------------------------------------------------------------------------
# Stage 1: Build gRPC from source and install into /usr/local
# ----------------------------------------------------------------------------------
# syntax=docker/dockerfile:1
FROM ubuntu:24.04 AS grpc-builder

# Disable interactive prompts during package installation.
ENV DEBIAN_FRONTEND=noninteractive

# Install essential packages including QEMU support (if building cross-platform)
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    pkg-config \
    libssl-dev \
    qemu-user-static \
    binfmt-support \
 && rm -rf /var/lib/apt/lists/*

# (Optional) Register QEMU if building for ARM64 on an x86_64 host.
RUN docker run --rm --privileged multiarch/qemu-user-static --reset -p yes || echo "QEMU already registered"

# Clone a newer version of gRPC (with its submodules, including abseil)
# In our experience, v1.51.1 tends to work better on ARM64.
RUN git clone --recurse-submodules -b v1.51.1 https://github.com/grpc/grpc.git /grpc

WORKDIR /grpc

# Create and move to the build directory.
RUN mkdir -p cmake/build
WORKDIR /grpc/cmake/build

# Configure CMake with C++ standard set to 17.
# You can add or adjust flags as needed.
RUN cmake \
    -DgRPC_INSTALL=ON \
    -DgRPC_BUILD_TESTS=OFF \
    -DCMAKE_CXX_STANDARD=17 \
    ../..

# Build and install.
RUN make -j$(nproc)
RUN make install

# ----------------------------------------------------------------------------------
# Stage 2: Build the Application
# ----------------------------------------------------------------------------------

FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install build tools and dependencies.
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    pkg-config \
    libprotobuf-dev \
    protobuf-compiler \
    libssl-dev \
    nlohmann-json3-dev

# Copy gRPC installation from previous stage.
COPY --from=grpc-builder /usr/local/ /usr/local/

# Set the LD_LIBRARY_PATH so that runtime can find the installed libraries.
ENV LD_LIBRARY_PATH=/usr/local/lib

# Set working directory for your project.
WORKDIR /app

# Copy all project files (including your proto folder, CMakeLists.txt, and source) into the container.
COPY . /app

# Create and move into a build directory, then configure and build the project.
# We pass the gRPC_DIR so that CMake can find the gRPC installation.
RUN mkdir -p build && cd build && \
    cmake -DgRPC_DIR=/usr/local/lib/cmake/grpc .. && \
    make -j$(nproc)

# ----------------------------------------------------------------------------------
# Stage 3: Create a minimal runtime container with the built binary.
# ----------------------------------------------------------------------------------

FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# Install runtime libraries (adjust versions as needed).
RUN apt-get update && apt-get install -y \
    libprotobuf-dev \
    protobuf-compiler \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Optionally, set the LD_LIBRARY_PATH so that the dynamic linker can find libKuksaClient.so.
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/bin

# Copy installed libraries (if needed) from the builder stage.
COPY --from=builder /usr/local/ /usr/local/

# Copy the built executable from the builder stage.
# Adjust the built executable name as necessary (here assumed as "KuksaDatabrokerClient").
COPY --from=builder /app/build/libKuksaClient.so /usr/local/bin
COPY --from=builder /app/build/KuksaDatabrokerClient /usr/local/bin/KuksaDatabrokerClient
COPY --from=builder /app/config.json /usr/local/bin/config.json
# COPY --from=builder /app/build /usr/local/bin/build

# Make sure the startup script has executable permissions.
# RUN sed -i -e 's/\r$//' *.sh
# RUN chmod +x *.sh

# (Optional) Verify that grpc_cpp_plugin exists.
RUN ls -l /usr/local/bin/grpc_cpp_plugin

# Expose any necessary port (optional)
# EXPOSE 55555

# Set the entrypoint to run your client.
ENTRYPOINT ["/usr/local/bin/KuksaDatabrokerClient"]
