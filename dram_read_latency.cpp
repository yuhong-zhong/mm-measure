#include <iostream>
#include <cstdlib>
#include <cstring>
#include <numa.h>

inline unsigned long single_latency(long *buffer) {
	unsigned int start_time_hi, start_time_lo;
	unsigned int end_time_hi, end_time_lo;

	asm volatile ("MOVQ %[buffer], %%r15\n"
		      "CPUID\n"
		      "RDTSC\n"
		      "MOVL %%edx, %0\n"
		      "MOVL %%eax, %1\n"
		      "MOVQ (%%r15), %%rbx\n"
		      "RDTSCP\n"
		      "MOVL %%edx, %2\n"
		      "MOVL %%eax, %3\n"
		      "CPUID\n"
		      : "=r" (start_time_hi), "=r" (start_time_lo), "=r" (end_time_hi), "=r" (end_time_lo)
		      : [buffer] "r" (buffer)
		      : "%rax", "%rbx", "%rcx", "%rdx", "%r15");

	unsigned long start_time = 0, end_time = 0;
	start_time += start_time_hi;
	start_time <<= 32;
	start_time |= start_time_lo;
	end_time += end_time_hi;
	end_time <<= 32;
	end_time |= end_time_lo;
	return end_time - start_time;
}

int main(int argc, char *argv[]) {
	if (argc != 4) {
		printf("Usage: %s <number of entries> <number of operations> <numa node>\n", argv[0]);
		exit(1);
	}

	long *data;
	long num_entry = atol(argv[1]), num_operation = atol(argv[2]);
	int numa_node = atoi(argv[3]);

	data = (long *)numa_alloc_onnode(sizeof(long) * num_entry, numa_node);
	if (!data) {
		printf("error: no memory\n");
		exit(1);
	}
	memset(data, 0, sizeof(long) * num_entry);

	unsigned long sum = 0;
	for (long i = 0; i < num_operation; ++i) {
		sum += single_latency(&data[rand() % num_entry]);
	}
	printf("%ld\n", sum / num_operation);
}
