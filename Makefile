test:
	cargo test --all-targets --locked

lint:
	cargo fmt --all -- --check
	cargo clippy --all-targets --locked -- -D warnings

confidence:
	cargo test --test confidence_yaml --locked

bench:
	cargo test --test perf_tpch --release --locked -- --nocapture
	cargo test --test bench_optimizer --release --locked -- --nocapture

bench-full:
	cargo test --test perf_tpch --release --locked -- --nocapture
	cargo test --test perf_tpch --release --locked -- bench_tpch_sf1  --ignored --nocapture
	cargo test --test perf_tpch --release --locked -- bench_tpch_sf10 --ignored --nocapture
	cargo test --test bench_optimizer --release --locked -- --nocapture

tpch-correctness:
	cargo test --test tpch_correctness --locked -- --nocapture

adversarial:
	cargo test --test adversarial_vectorized --features simd --locked
	cargo test --test adversarial_optimizer --locked
	cargo test --test adversarial_mvcc --locked
	cargo test --test adversarial_raft --locked

cluster-test:
	docker compose up -d --wait
	cargo test --test raft_correctness --locked -- --nocapture
	docker compose down

clean:
	cargo clean
	if exist tmp_* rmdir /s /q tmp_* 2>nul

clean-db:
	if exist *.db del /q *.db 2>nul
	if exist *.sst del /q *.sst 2>nul

clean-test-artifacts:
	if exist tmp_* rmdir /s /q tmp_* 2>nul
	if exist test_db* rmdir /s /q test_db* 2>nul

gen-certs:
	mkdir certs 2>nul || cd .
	openssl req -x509 -newkey rsa:4096 -keyout certs\server.key -out certs\server.crt -days 365 -nodes -subj "/CN=neuralbase-dev"
	@echo Certificates written to certs\\server.key and certs\\server.crt
gen-cluster-certs:
	mkdir certs 2>nul || cd .
	openssl req -x509 -newkey rsa:4096 -keyout certs\ca.key -out certs\ca.crt -days 3650 -nodes -subj "/CN=neuralbase-ca"
	openssl req -newkey rsa:2048 -keyout certs\node1.key -out certs\node1.csr -nodes -subj "/CN=node1"
	openssl x509 -req -in certs\node1.csr -CA certs\ca.crt -CAkey certs\ca.key -CAcreateserial -out certs\node1.crt -days 365
	openssl req -newkey rsa:2048 -keyout certs\node2.key -out certs\node2.csr -nodes -subj "/CN=node2"
	openssl x509 -req -in certs\node2.csr -CA certs\ca.crt -CAkey certs\ca.key -CAcreateserial -out certs\node2.crt -days 365
	openssl req -newkey rsa:2048 -keyout certs\node3.key -out certs\node3.csr -nodes -subj "/CN=node3"
	openssl x509 -req -in certs\node3.csr -CA certs\ca.crt -CAkey certs\ca.key -CAcreateserial -out certs\node3.crt -days 365
	@echo Cluster certs written to certs\: ca.crt, node1/2/3.crt+key