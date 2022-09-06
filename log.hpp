#ifndef LOG_H
#define LOG_H
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <libpmem.h>
#include <sched.h>
#include <sys/mman.h>
#include <thread>

class CircularFifo{
public:
  enum {Capacity = 30 * 128 * 1024};

  void tail_commit() {
    int count = (_tail->load(std::memory_order_acquire) - _head->load(std::memory_order_acquire)) / 30;
    for (int i = 0; i < count; ++i)
      pop();
    for (size_t i = *_head; i < _tail->load(std::memory_order_acquire); ++i) {
      pmem_memcpy_persist(pmem_users + i, _array + i % Capacity, sizeof(User));
    }
    *_head = _tail->load(std::memory_order_acquire);
  }

  CircularFifo(const std::string &filename, Data *data) : _tail(0), _head(0), done_count(0){
    char *map_ptr = reinterpret_cast<char *>(map_file(filename.c_str(), Capacity * sizeof(User) + 64 + Capacity));
    _head = reinterpret_cast<std::atomic<uint_fast32_t> *>(map_ptr);
    _tail = reinterpret_cast<std::atomic<uint_fast32_t> *>(map_ptr + 8);
    is_readable = reinterpret_cast<volatile uint8_t *>(map_ptr + 64);
    _array = reinterpret_cast<User *>(map_ptr + 64 + Capacity);
    this->data = data;
    this->pmem_users = data->get_pmem_users();
    tail_commit(); // 把上一次退出没提交完的提交完
    
    exited = false;
    auto write_task = [&] {
      while (!exited) {
        while (done_count < 3 && !exited) {
          std::this_thread::yield();
        }
        if (exited)
          break;
        pop();
      }
    };
    writer_thread = new std::thread(write_task);
  }
  
  ~CircularFifo() {
    exited = true;
    writer_thread->join();
    // tail commit
    tail_commit();
    munmap((void *)_head, Capacity * sizeof(User) + 64 + Capacity);
  }

  // 如果index > *_head，说明还没有刷下去，这个时候可以从索引里面读
  // TODO: 读着读者被刷走了怎么办呢，再加一个volatile表示不要被刷走
  // 两个offset转换好像有原子性的问题，需要自己给定一个唯一的转化函数,要给索引一个index
  size_t push(const User& item)
  {
    size_t current_tail = _tail->fetch_add(1, std::memory_order_acquire);
    while (current_tail - _head->load(std::memory_order_acquire) >= Capacity) {
      // 这里可以调用pmem_persist，就不用等
      sched_yield();
    }

    // 这里可以不用原子指令吗，不希望is_readable设置期间发生调度
    _array[current_tail % Capacity] = item;
    is_readable[current_tail % Capacity] = 1;
    // 有什么办法能确保这30个全部写完了呢
    if ((current_tail + 1) % 30 == 0)
      done_count.fetch_add(1, std::memory_order_acquire);
    return current_tail + 1;
  }

  bool check_readable(int index) {
    static const uint32_t value = 0x01010101;
    uint32_t *base = (uint32_t *)(is_readable + (index % Capacity));
    for (int i = 0; i < 7; ++i) {
      if (base[i] != value)
        return true;
    }
    uint8_t *res = (uint8_t *)base;
    if (res[28] && res[29])
      return false;
    return true;
  }

  // 在pop就不要任何检查了
  void pop()
  {
    size_t index = _head->load(std::memory_order_relaxed);
    // 等30个全部写完
    while (check_readable(index)) {
      sched_yield();
    }
    /* for (int i = 0; i < 30; ++i) { */
    /*   assert(is_readable[(index + i) % Capacity] == 1); */
    /* } */

    pmem_memcpy_persist(pmem_users + index, _array + index % Capacity, sizeof(User) * 30);
    memset((void *)&is_readable[index % Capacity], 0, 30);
    _head->store(index + 30, std::memory_order_release);
    done_count.fetch_sub(1, std::memory_order_acquire);
  }

  const User *read(uint32_t index) {
    if (!data->get_flag(index))
      return nullptr;
    index -= 1;
    if (index >= *_head) 
      return _array + index % Capacity;
    return data->data_read(index + 1);
  }

private:
  std::atomic<uint_fast32_t>  *_tail; // 当next_location用就好了
  std::atomic<uint_fast32_t> *_head;
  volatile uint8_t *is_readable;
  User *_array;
  std::atomic<size_t> done_count; // 这个需要放文件里面吗，感觉好像有问题
  Data *data;
  User *pmem_users;
  volatile bool exited;
  std::thread *writer_thread;
public:
};
#endif

