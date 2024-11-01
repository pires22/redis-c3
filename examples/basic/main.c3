module main;
import std::io;
import redis;
import std::collections::object;

fn void main() {
    String host = "localhost";
    uint port = 6379;
    RedisClient* redis_client = redis::new_client(host, port, debug: true);
    if (catch error = redis_client.connect()) {
        io::printfn("failed to connect to redis: %s", error);
        return;
    }
    io::printn("connected to redis");
    if (try response = redis_client.send("SET", "foo", "bar")) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.str);
        free(response);
    }
    if (try response = redis_client.send("EXISTS", "foo")) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.integer);
        free(response);
    }
    if (try response = redis_client.send("GETDEL", "foo")) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.str);
        free(response);
    }
    if (try response = redis_client.send("EXISTS", "foo")) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.integer);
        free(response);
    }
    if (try response = redis_client.send("RPUSH", "hello", "foo", "bar", "baz", 1, 2, 3)) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.str);
        free(response);
    }
    if (try response = redis_client.send("LRANGE", "hello", 0, -1)) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.len);
        free(response);
    }
    if (try response = redis_client.send("LPOP", "hello")) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.str);
        free(response);
    }
    if (try response = redis_client.send("LRANGE", "hello", 0, -1)) {
        io::printfn("response type: %s", response.type);
        io::printfn("response value: %s", response.len);
        free(response);
    }
    free(redis_client);
}
