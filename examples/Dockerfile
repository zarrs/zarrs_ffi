FROM rust:1.80.0

RUN apt update && apt install -y cmake

WORKDIR /example

COPY . .

WORKDIR /example/build
RUN cmake -DCMAKE_BUILD_TYPE=Release .. && make
ENTRYPOINT [ "./main" ]
