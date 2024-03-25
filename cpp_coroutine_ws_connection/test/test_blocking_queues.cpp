
#include <format>
#include <iostream>
#include "blocking_queues.h"
#include <boost/test/unit_test.hpp>

namespace ws_toolkit::test {

size_t get_ops_per_sec_1p1c(size_t q_size, size_t iteration_count) {
	BlockingRingBuffer<uint64_t> q(q_size);
	const size_t expected_result = iteration_count * (iteration_count - 1) / 2;

	uint64_t result = 0;
	std::thread thr_consumer([&](){
		auto c = iteration_count;
		while (c-- > 0) {
			q.poll([&result](uint64_t* ptr){
				result += *ptr;
			});
		}
	});
	const auto start = std::chrono::high_resolution_clock::now();
	std::thread thr_producer([&](){
		for (uint64_t i = 0; i < iteration_count; ++i) {
			q.offer([&i](uint64_t* ptr){
				*ptr = i;
			});
		}
	});
	thr_producer.join();
	thr_consumer.join();
	
    const auto elapsed = std::chrono::high_resolution_clock::now() - start;
    const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
	BOOST_CHECK_EQUAL(expected_result, result);
	return iteration_count * 1000 * 1000 / elapsed_us;
}

BOOST_AUTO_TEST_CASE(benchmark_blocking_queue_1p1c) {
	size_t run_count = 4;
	size_t q_size = 1024;
	size_t iteration_count = 100 * 1000 * 1000;

	std::cout << "BlockingRingBuffer Benchmark\r\n";
	for (size_t i = 0; i < run_count; ++i) {
		std::cout << std::format("#{} {}  ops/sec\r\n", i + 1, get_ops_per_sec_1p1c(q_size, iteration_count));
	}

}

size_t get_ops_per_sec_multicast(size_t q_size, size_t iteration_count, size_t consumer_count) {
	SpmcQueue<uint64_t> q(q_size);
	const size_t expected_result = iteration_count * (iteration_count - 1) / 2;
	std::vector<std::thread> thr_consumers;
	std::vector<decltype(q)::ConsumerHandler*> handlers;
	// create consumer handlers first
	for (size_t i = 0; i < consumer_count; ++i) {
		handlers.push_back(q.createConsumer());
	}
	for (size_t i = 0; i < consumer_count; ++i) {
		thr_consumers.emplace_back([&](int consumer_idx) {
			uint64_t result = 0;
			auto* hdr = handlers[consumer_idx];
			uint64_t count = 0;
			while (count < iteration_count) {
				auto rg = hdr->poll(1);
				for (const auto& n : rg) {
					result += n;
				}
				hdr->markConsumed(rg);
				count += rg.count();
			}
			if (result != expected_result) {
				std::cerr << std::format("#{} bad result: expected {}, got {}\r\n", i, expected_result, result) << std::flush;
			}
		}, i);
	}
	const auto start = std::chrono::high_resolution_clock::now();
	std::thread thr_producer([&](){
		uint64_t i = 0;
		// publish in batches
		while (i < iteration_count - q_size) {
			auto rg = q.getAvailableRange(1);
			for (auto& n : rg) {
				n = i++;
			}
			q.publish(rg);
		}
		while (i < iteration_count) {
			auto idx = q.nextAvailableIndex();
			q[idx] = i;
			q.publish(i);
			++i;
		}
	});
	for (auto& t : thr_consumers) {
		t.join();
	}
	thr_producer.join();
	
    const auto elapsed = std::chrono::high_resolution_clock::now() - start;
    const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
	return iteration_count * 1000 * 1000 / elapsed_us;
}
	
BOOST_AUTO_TEST_CASE(benchmark_blocking_queue_multicast) {
	size_t run_count = 4;
	size_t consumer_count = 2;
	size_t q_size = 256;
	size_t iteration_count = 10 * 1000 * 1000;

	std::cout << "MPMC Queue Benchmark\r\n";
	for (size_t i = 0; i < run_count; ++i) {
		std::cout << std::format("#{} {}  ops/sec\r\n", i + 1, get_ops_per_sec_multicast(q_size, iteration_count, consumer_count));
	}
}

}
