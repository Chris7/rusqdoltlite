#define _GNU_SOURCE

/* Loaded only by an isolated test child to inject one deterministic EINTR at
 * each accepted native socket's receive and send boundary. The Rust parent
 * asserts the marker records, so the test cannot pass without exercising both.
 */

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

typedef int (*accept_fn)(int, struct sockaddr *, socklen_t *);
typedef ssize_t (*recv_fn)(int, void *, size_t, int);
typedef ssize_t (*send_fn)(int, const void *, size_t, int);

static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;
static accept_fn real_accept;
static recv_fn real_recv;
static send_fn real_send;
static _Atomic int accepted_fd = -1;
static _Atomic int interrupt_next_recv;
static _Atomic int interrupt_next_send;

static void resolve_socket_functions(void) {
  real_accept = (accept_fn)dlsym(RTLD_NEXT, "accept");
  real_recv = (recv_fn)dlsym(RTLD_NEXT, "recv");
  real_send = (send_fn)dlsym(RTLD_NEXT, "send");
}

static void record_interruption(const char *operation) {
  const char *marker = getenv("RUSQ_REMOTE_EINTR_MARKER");
  int fd;
  size_t length;

  if (!marker) return;
  fd = open(marker, O_WRONLY | O_CREAT | O_APPEND, 0600);
  if (fd < 0) return;
  length = strlen(operation);
  (void)write(fd, operation, length);
  (void)write(fd, "\n", 1);
  (void)close(fd);
}

int accept(int fd, struct sockaddr *address, socklen_t *address_length) {
  int accepted;

  pthread_once(&resolve_once, resolve_socket_functions);
  accepted = real_accept(fd, address, address_length);
  if (accepted >= 0) {
    atomic_store_explicit(&accepted_fd, accepted, memory_order_release);
    atomic_store_explicit(&interrupt_next_recv, 1, memory_order_release);
    atomic_store_explicit(&interrupt_next_send, 1, memory_order_release);
  }
  return accepted;
}

ssize_t recv(int fd, void *buffer, size_t length, int flags) {
  pthread_once(&resolve_once, resolve_socket_functions);
  if (fd == atomic_load_explicit(&accepted_fd, memory_order_acquire)
      && atomic_exchange_explicit(&interrupt_next_recv, 0, memory_order_acq_rel)) {
    record_interruption("recv");
    errno = EINTR;
    return -1;
  }
  return real_recv(fd, buffer, length, flags);
}

ssize_t send(int fd, const void *buffer, size_t length, int flags) {
  pthread_once(&resolve_once, resolve_socket_functions);
  if (fd == atomic_load_explicit(&accepted_fd, memory_order_acquire)
      && atomic_exchange_explicit(&interrupt_next_send, 0, memory_order_acq_rel)) {
    record_interruption("send");
    errno = EINTR;
    return -1;
  }
  return real_send(fd, buffer, length, flags);
}
