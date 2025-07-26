#include "zarrs.h"

#include <iostream>
#include <memory>

int main() {
  const char *tmp_path = getenv("TMP_PATH_WRITE_RUST_READ_C");
  ZarrsStorage storage = nullptr;
  zarrs_assert(zarrsCreateStorageFilesystem(tmp_path, &storage));
  assert(storage);

  ZarrsArray array = nullptr;
  zarrs_assert(zarrsOpenArrayRW(storage, "/array", &array));
  assert(array);

  // Update a subset
  size_t subset_start[] = {1, 1};
  size_t subset_shape[] = {2, 2};
  float subset_elements[] = {-1.0f, -2.0f, -3.0f, -4.0f};
  uint8_t *subset_bytes = reinterpret_cast<uint8_t *>(subset_elements);
  zarrs_assert(zarrsArrayStoreSubset(array, 2, subset_start, subset_shape, 4 * sizeof(float), subset_bytes));

  // Get the chunk size (in bytes)
  size_t indices[] = {0, 0};
  size_t chunk_size;
  zarrs_assert(zarrsArrayGetChunkSize(array, 2, indices, &chunk_size));
  std::cout << chunk_size << std::endl;

  // Get chunk elements
  std::unique_ptr<float[]> chunk_elements(new float[chunk_size / sizeof(float)]);
  zarrs_assert(zarrsArrayRetrieveChunk(array, 2, indices, chunk_size, reinterpret_cast<uint8_t*>(chunk_elements.get())));

  // Print the elements
  for (size_t i = 0; i < chunk_size / sizeof(float); ++i) {
    std::cout << (i == 0 ? "" : " ") << chunk_elements[i];
  }
  std::cout << std::endl;
  chunk_elements.reset();

  // Cleanup
  zarrs_assert(zarrsDestroyArray(array));
  zarrs_assert(zarrsDestroyStorage(storage));
}