#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# Define the target platforms.
# You can extend the array with other platforms as needed.
# platforms=("linux/amd64")
platforms=("linux/arm64" "linux/amd64")

# Specific - multi-arch build
# - Enable Docker Buildx (if not already enabled) > Run the docker container
docker buildx create --name kuksa-databroker-client-multiarch-build --use

# Loop through each platform.
for platform in "${platforms[@]}"; do
  # Extract the architecture part for tagging and output folder.
  arch=$(echo "$platform" | cut -d'/' -f2)

  # Define your image tag. For example, "kuksa-databroker-client:arm64"
  image_tag="kuksa-databroker-client:${arch}"

  echo "=================================================="
  echo "Building image for platform ${platform} with tag ${image_tag}"

  # Build the image using buildx and load it into Docker.
  docker buildx build --platform "${platform}" -t "${image_tag}" --load .

  # Create an output directory for this architecture if it does not exist.
  output_dir="$PWD/lib/${arch}"
  mkdir -p "${output_dir}"

  # Create a temporary container from the built image.
  container_id=$(docker create "${image_tag}")

  echo "Extracting /usr/local/bin/libKuksaClient.so from container ${container_id}"

  # Copy the shared library from the container to the host's output directory.
  docker cp "${container_id}:/usr/local/bin/libKuksaClient.so" "${output_dir}/"

  # Remove the temporary container.
  docker rm "${container_id}" > /dev/null

  echo "Library copied to ${output_dir}"
  echo "=================================================="
done

echo "Build and copy process completed successfully."
