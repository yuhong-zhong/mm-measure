#include <iostream>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <thread>
#include <atomic>
#include <numa.h>

using namespace std;


void random_read_batch(long *buffer, long start_index, long buffer_len_mask, long batch_size) {
	asm volatile ("MOVQ %[buffer], %%r15\n"            /* r15: long *buffer */
	              "MOVQ %[start_index], %%r14\n"       /* r14: size_t offset */
	              "MOVQ %[batch_size], %%r13\n"        /* r13: size_t remain */
	              "MOVQ %[buffer_len_mask], %%r12\n"   /* r12: long buffer_len_mask */
	              "loop:\n"
	              "MOVQ (%%r15, %%r14, 8), %%rbx\n"    /* read from memory */
	              "MOVQ %%r14, %%rax\n"                /* randomly generate next position */
	              "MULQ $1103515245\n"
	              "ADDQ $12345, %%rax\n"
	              "ANDQ %%r12, %%rax\n"
	              "MOVQ %%rax, %%r14\n"
	              "DECQ %%r13\n"
	              "JNZ loop\n"
	:
	: [buffer] "r" (buffer), [start_index] "r" (start_index), 
	  [buffer_len_mask] "r" (buffer_len_mask), [batch_size] "r" (batch_size)
	: "%rax", "%rbx", "%rcx", "%rdx", "%r12", "%r13", "%r14", "%r15");
}

void random_write_batch(long *buffer, long start_index, long buffer_len_mask, long batch_size) {
	asm volatile ("MOVQ %[buffer], %%r15\n"            /* r15: long *buffer */
	              "MOVQ %[start_index], %%r14\n"       /* r14: size_t offset */
	              "MOVQ %[batch_size], %%r13\n"        /* r13: size_t remain */
	              "MOVQ %[buffer_len_mask], %%r12\n"   /* r12: long buffer_len_mask */
	              "loop:\n"
	              "MOVQ %%rbx, (%%r15, %%r14, 8)\n"    /* write to memory */
	              "MOVQ %%r14, %%rax\n"                /* randomly generate next position */
	              "MULQ $1103515245\n"
	              "ADDQ $12345, %%rax\n"
	              "ANDQ %%r12, %%rax\n"
	              "MOVQ %%rax, %%r14\n"
	              "DECQ %%r13\n"
	              "JNZ loop\n"
	:
	: [buffer] "r" (buffer), [start_index] "r" (start_index), 
	  [buffer_len_mask] "r" (buffer_len_mask), [batch_size] "r" (batch_size)
	: "%rax", "%rbx", "%rcx", "%rdx", "%r12", "%r13", "%r14", "%r15");
}

void random_read_write_batch(long *buffer, long start_index, long buffer_len_mask, long batch_size) {
	asm volatile ("MOVQ %[buffer], %%r15\n"            /* r15: long *buffer */
	              "MOVQ %[start_index], %%r14\n"       /* r14: size_t offset */
	              "MOVQ %[batch_size], %%r13\n"        /* r13: size_t remain */
	              "MOVQ %[buffer_len_mask], %%r12\n"   /* r12: long buffer_len_mask */
	              "loop:\n"
		      "TESTQ $1, %%r13\n"                  /* decide read or write */
		      "JNZ write\n"
		      "MOVQ (%%r15, %%r14, 8), %%rbx\n"    /* read from memory */
		      "JMP loop_end\n"
		      "write:\n"
	              "MOVQ %%rbx, (%%r15, %%r14, 8)\n"    /* write to memory */
		      "loop_end:\n"
	              "MOVQ %%r14, %%rax\n"                /* randomly generate next position */
	              "MULQ $1103515245\n"
	              "ADDQ $12345, %%rax\n"
	              "ANDQ %%r12, %%rax\n"
	              "MOVQ %%rax, %%r14\n"
	              "DECQ %%r13\n"
	              "JNZ loop\n"
	:
	: [buffer] "r" (buffer), [start_index] "r" (start_index), 
	  [buffer_len_mask] "r" (buffer_len_mask), [batch_size] "r" (batch_size)
	: "%rax", "%rbx", "%rcx", "%rdx", "%r12", "%r13", "%r14", "%r15");
}

void seq_read_batch(long *buffer, long start_index, long buffer_len_mask, long batch_size) {
	asm volatile ("MOVQ %[buffer], %%r15\n"            /* r15: long *buffer */
	              "MOVQ %[start_index], %%r14\n"       /* r14: size_t offset */
	              "MOVQ %[batch_size], %%r13\n"        /* r13: size_t remain */
	              "MOVQ %[buffer_len_mask], %%r12\n"   /* r12: long buffer_len_mask */
	              "loop:\n"
	              "MOVQ (%%r15, %%r14, 8), %%rbx\n"    /* read from memory */
	              "INCQ %%r14\n"                       /* get next offset */
	              "ANDQ %%r12, %%r14\n"
	              "DECQ %%r13\n"
	              "JNZ loop\n"
	:
	: [buffer] "r" (buffer), [start_index] "r" (start_index), 
	  [buffer_len_mask] "r" (buffer_len_mask), [batch_size] "r" (batch_size)
	: "%rax", "%rbx", "%rcx", "%rdx", "%r12", "%r13", "%r14", "%r15");
}

void seq_write_batch(long *buffer, long start_index, long buffer_len_mask, long batch_size) {
	asm volatile ("MOVQ %[buffer], %%r15\n"            /* r15: long *buffer */
	              "MOVQ %[start_index], %%r14\n"       /* r14: size_t offset */
	              "MOVQ %[batch_size], %%r13\n"        /* r13: size_t remain */
	              "MOVQ %[buffer_len_mask], %%r12\n"   /* r12: long buffer_len_mask */
	              "loop:\n"
	              "MOVQ %%rbx, (%%r15, %%r14, 8)\n"    /* write to memory */
		      "INCQ %%r14\n"                       /* get next offset */
	              "ANDQ %%r12, %%r14\n"
	              "DECQ %%r13\n"
	              "JNZ loop\n"
	:
	: [buffer] "r" (buffer), [start_index] "r" (start_index), 
	  [buffer_len_mask] "r" (buffer_len_mask), [batch_size] "r" (batch_size)
	: "%rax", "%rbx", "%rcx", "%rdx", "%r12", "%r13", "%r14", "%r15");
}

void seq_read_write_batch(long *buffer, long start_index, long buffer_len_mask, long batch_size) {
	asm volatile ("MOVQ %[buffer], %%r15\n"            /* r15: long *buffer */
	              "MOVQ %[start_index], %%r14\n"       /* r14: size_t offset */
	              "MOVQ %[batch_size], %%r13\n"        /* r13: size_t remain */
	              "MOVQ %[buffer_len_mask], %%r12\n"   /* r12: long buffer_len_mask */
	              "loop:\n"
		      "TESTQ $1, %%r13\n"                  /* decide read or write */
		      "JNZ write\n"
		      "MOVQ (%%r15, %%r14, 8), %%rbx\n"    /* read from memory */
		      "JMP loop_end\n"
		      "write:\n"
	              "MOVQ %%rbx, (%%r15, %%r14, 8)\n"    /* write to memory */
		      "loop_end:\n"
		      "INCQ %%r14\n"                       /* get next offset */
	              "ANDQ %%r12, %%r14\n"
	              "DECQ %%r13\n"
	              "JNZ loop\n"
	:
	: [buffer] "r" (buffer), [start_index] "r" (start_index), 
	  [buffer_len_mask] "r" (buffer_len_mask), [batch_size] "r" (batch_size)
	: "%rax", "%rbx", "%rcx", "%rdx", "%r12", "%r13", "%r14", "%r15");
}

enum thread_type {
	RANDOM_READ,
	RANDOM_WRITE,
	RANDOM_READ_WRITE,
	SEQ_READ,
	SEQ_WRITE,
	SEQ_READ_WRITE
};

void thread_fn(enum thread_type type, int thread_index,
               long num_entry, long num_op, long batch_size, int numa_node,
               atomic<bool> *terminate, double *result_arr) {
	long *buffer = (long *)numa_alloc_onnode(sizeof(long) * num_entry, numa_node);
	if (!buffer) {
		printf("Error: failed to allocate buffer\n");
		exit(1);
	}
	memset(buffer, 0, sizeof(long) * num_entry);
	long buffer_len_shift = (long) floor(log2((double) num_entry));
	long buffer_len_mask = (1 << buffer_len_shift) - 1;

	long cur_num_op = 0;
	chrono::time_point<chrono::steady_clock> start_time = chrono::steady_clock::now();
	while (!atomic_load(terminate) && cur_num_op < num_op) {
		switch (type) {
		case RANDOM_READ:
			random_read_batch(buffer, thread_index, buffer_len_mask, batch_size);
			break;
		case RANDOM_WRITE:
			random_write_batch(buffer, thread_index, buffer_len_mask, batch_size);
			break;
		case RANDOM_READ_WRITE:
			random_read_write_batch(buffer, thread_index, buffer_len_mask, batch_size);
			break;
		case SEQ_READ:
			seq_read_batch(buffer, 0, buffer_len_mask, batch_size);
			break;
		case SEQ_WRITE:
			seq_write_batch(buffer, 0, buffer_len_mask, batch_size);
			break;
		case SEQ_READ_WRITE:
			seq_read_write_batch(buffer, 0, buffer_len_mask, batch_size);
			break;
		default:
			printf("Error: unrecognized op type\n");
			exit(1);
		}
		cur_num_op += batch_size;
	}
	chrono::time_point<chrono::steady_clock> end_time = chrono::steady_clock::now();
	atomic_store(terminate, true);

	result_arr[thread_index] = (double) cur_num_op
		/ (((double) chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count())
		   / 1e9);
}

int main(int argc, char *argv[]) {
	if (argc < 6) {
		printf("Usage: %s <number of entries> <number of operations> <batch size> <numa node> <thread_op> ...\n", argv[0]);
		exit(1);
	}

	long num_entry = atol(argv[1]), num_op = atol(argv[2]), batch_size = atol(argv[3]);
	int numa_node = atoi(argv[4]);
	long num_thread = argc - 5;

	char **thread_type_arr = argv + 5;
	thread *thread_arr = new thread[num_thread];
	double *result_arr = new double[num_thread];
	atomic<bool> terminate(false);

	for (int thread_index = 0; thread_index < num_thread; ++thread_index) {
		enum thread_type type;
		if (strcmp("RANDOM_READ", thread_type_arr[thread_index]) == 0) {
			type = RANDOM_READ;
		} else if (strcmp("RANDOM_WRITE", thread_type_arr[thread_index]) == 0) {
			type = RANDOM_WRITE;
		} else if (strcmp("RANDOM_READ_WRITE", thread_type_arr[thread_index]) == 0) {
			type = RANDOM_READ_WRITE;
		} else if (strcmp("SEQ_READ", thread_type_arr[thread_index]) == 0) {
			type = SEQ_READ;
		} else if (strcmp("SEQ_WRITE", thread_type_arr[thread_index]) == 0) {
			type = SEQ_WRITE;
		} else if (strcmp("SEQ_READ_WRITE", thread_type_arr[thread_index]) == 0) {
			type = SEQ_READ_WRITE;
		} else {
			printf("Error: recognized thread type\n");
			exit(1);
		}

		thread_arr[thread_index] = thread(thread_fn, type, thread_index, num_entry,
		                                  num_op, batch_size, numa_node, &terminate,
		                                  result_arr);
	}
	for (int thread_index = 0; thread_index < num_thread; ++thread_index) {
		thread_arr[thread_index].join();
	}
	for (int thread_index = 0; thread_index < num_thread; ++thread_index) {
		printf("thread %d - %s: %f\n", thread_index, thread_type_arr[thread_index], result_arr[thread_index]);
	}
	return 0;
}
