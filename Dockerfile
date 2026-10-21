# --- build stage ---
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      g++ cmake make libhiredis-dev libpq-dev nlohmann-json3-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*
# cpp-httplib is a single header; pin an exact release
ADD https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.18.1/httplib.h /usr/local/include/httplib.h
WORKDIR /src
COPY CMakeLists.txt .
COPY src src
COPY tests tests
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j && ./build/test_task_logic

# --- runtime stage: just the binaries and their shared libraries ---
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libhiredis0.14 libpq5 \
 && rm -rf /var/lib/apt/lists/* && useradd -r scheduler
COPY --from=build /src/build/scheduler_api /src/build/scheduler_worker /usr/local/bin/
USER scheduler
EXPOSE 8080
CMD ["scheduler_api"]
