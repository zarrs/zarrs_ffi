FROM rust:1.82.0

RUN apt update && apt install -y cmake

COPY . .

RUN cmake -B build
RUN cmake --build build
ENTRYPOINT [ "./build/main" ]
