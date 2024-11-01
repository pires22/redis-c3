module redis;
import std::net::tcp;
import std::io;
import std::thread;
import std::collections::list;
import std::collections::map;

struct RedisClient {
    bool debug;
    TcpSocket socket;
    bool connected;
    String host;
    uint port;
}

enum RedisReplyType {
    REDIS_REPLY_STATUS,
    REDIS_REPLY_ERROR,
    REDIS_REPLY_STRING,
    REDIS_REPLY_INTEGER,
    REDIS_REPLY_DOUBLE,
    REDIS_REPLY_NULL,
    REDIS_REPLY_ARRAY,
    REDIS_REPLY_MAP,
    REDIS_REPLY_UNKNOWN
}

struct RedisReply {
    char tag;
    RedisReplyType type;
    DString str;
    usz len;
    long integer;
    double doubl;
    List(<RedisReply*>) elements;
    HashMap(<String, RedisReply*>) map;
}

fn RedisClient* new_client(String host, uint port, bool debug = false) {
    RedisClient* client = (RedisClient*)malloc(RedisClient.sizeof);
    client.host = host;
    client.port = port;
    client.debug = debug;
    return client;
}

fn void! RedisClient.connect(&self) {
    TcpSocket! client = std::net::tcp::connect(self.host, self.port, ip_protocol: std::net::IpProtocol.IPV4);
    if (catch reason = client) {
        return reason?;
    }
    self.connected = true;
    self.socket = client;
    self.send("HELLO", 3)!!;
}

fn void RedisClient.send_and_forget(&self, String packet) {
    if (catch reason = self.socket.write(packet)) {
        io::printfn("failed to publish packet: %s", reason);
        return;
    }

    self.wait_for_response();
}

fn DString RedisClient.wait_for_response(&self) {
    char[1024] buffer;
    DString response = dstring::temp_new();
    while (!response.str_view().ends_with("\r\n")) {
        usz! read_size = self.socket.read(&buffer);
        if (catch error = read_size) {
            io::printfn("failed to read from client: %s", error);
            break;
        }
        if (read_size == 0) {
            io::printn("connection closed");
            break;
        }
        if (try response.write(buffer[0:read_size])) {
        }
    }
    return response;
}

fn RedisReply*! RedisClient.send(&self, args...) {
    DString bulk_array = dstring::new();
    int appended = 0;
    foreach(arg: args) {
        switch (arg.type)
        {
            case int.typeid:
                int casted = *anycast(arg, int)!!;
                DString casted_dstring = dstring::new();
                casted_dstring.append_string(int_to_string(casted));
                bulk_array.append_char('$');
                bulk_array.append_string(int_to_string((int)casted_dstring.len()));
                bulk_array.append_chars("\r\n");
                bulk_array.append_string(casted_dstring);
                bulk_array.append_chars("\r\n");
                appended +=1;
                break;
            case String.typeid:
                DString casted = dstring::new(*anycast(arg, String)!!);
                bulk_array.append_char('$');
                bulk_array.append_string(int_to_string((int)casted.len()));
                bulk_array.append_chars("\r\n");
                bulk_array.append_string(casted);
                bulk_array.append_chars("\r\n");
                appended += 1;
                break;
            case double.typeid:
                double casted = *anycast(arg, double)!!;
                DString casted_dstring = dstring::new();
                casted_dstring.append_string(double_to_string(casted));
                bulk_array.append_char('$');
                bulk_array.append_string(int_to_string((int)casted_dstring.len()-1));
                bulk_array.append_chars("\r\n");
                bulk_array.append_string(casted_dstring);
                bulk_array.append_chars("\r\n");
                appended += 1;
                break;
            case bool.typeid:
                bool casted = *anycast(arg, bool)!!;
                DString casted_dstring = dstring::new();
                if (casted) {
                    casted_dstring.append_chars("true");
                } else {
                    casted_dstring.append_chars("false");
                }
                bulk_array.append_char('$');
                bulk_array.append_string(int_to_string((int)casted_dstring.len()));
                bulk_array.append_chars("\r\n");
                bulk_array.append_string(casted_dstring);
                bulk_array.append_chars("\r\n");
                appended +=1;
                break;
            default:
                io::printfn("type not implemented for arg: %s", arg);
                break;
        }
    }
    DString packet = dstring::new();
    packet.append_char('*');
    packet.append_string(int_to_string(appended));
    packet.append_chars("\r\n");
    packet.append_string(bulk_array);
    if (catch reason = self.socket.write(packet.str_view())) {
        io::printfn("failed to publish packet: %s", reason);
        return reason?;
    }
    packet.free();

    DString response = self.wait_for_response();

    RedisReply* reply = create_reply(&response);

    reply.parse_response(&response);
    
    return reply;
}

fn RedisReply* create_reply(DString* buffer, int reappend_tag = 1) {
    RedisReply* reply = mem::new(RedisReply);
    DString tag = get_element_from_buffer(buffer);
    reply.tag = tag.str_view()[0];
    if (reappend_tag) {
        tag.append_chars("\r\n");
        buffer.insert_at(0, tag.str_view()[1..tag.len()-1]);
    }
    return reply;
}

fn void RedisReply.parse_response(&self, DString* buffer, int first_step = 1) {
    switch (self.tag) {
        case '+':
        case '-':
            DString element = get_element_from_buffer(buffer);
            self.type = RedisReplyType.REDIS_REPLY_STATUS;
            self.str = element.copy();
            self.len = element.len();
            return;
        case '$':
            if (first_step) {
                chop_buffer(buffer);
            }
            DString element = get_element_from_buffer(buffer);
            self.type = RedisReplyType.REDIS_REPLY_STRING;
            self.str = element.copy();
            self.len = element.len();
            return;
        case ':':
            DString element = get_element_from_buffer(buffer);
            self.type = RedisReplyType.REDIS_REPLY_INTEGER;
            self.integer = element.str_view().trim().to_long()!!;
            return;
        case ',':
            DString element = get_element_from_buffer(buffer);
            self.type = RedisReplyType.REDIS_REPLY_DOUBLE;
            self.doubl = element.str_view().trim().to_double()!!;
            return;
        case '_':
            DString element = get_element_from_buffer(buffer);
            self.type = RedisReplyType.REDIS_REPLY_NULL;
            return;
        case '*':
            DString length_dstring = get_element_from_buffer(buffer);
            int str_len ;
            if (length_dstring.len() > 0) {
                str_len = length_dstring.str_view().to_int()!!;
            }
            self.type = RedisReplyType.REDIS_REPLY_ARRAY;
            self.len = str_len;
            while(buffer.len() != 0) {
                DString element = get_element_from_buffer(buffer);
                RedisReply* new_reply = create_reply(&element);
                new_reply.parse_response(buffer, 0);
                self.elements.push(new_reply);
            }
            return;
        case '%':
            DString length_dstring = get_element_from_buffer(buffer);
            int str_len = length_dstring.str_view().to_int()!!;
            self.type = RedisReplyType.REDIS_REPLY_MAP;
            self.len = str_len;
            self.map.new_init();
            while(buffer.len() != 0) {
                DString element = get_element_from_buffer(buffer);
                RedisReply* key = create_reply(&element);
                key.parse_response(buffer, 0);
                element = get_element_from_buffer(buffer);
                RedisReply* value;
                if (element.str_view()[0] == ':') {
                    value = create_reply(&element);
                    value.parse_response(&element);
                } else {
                    value = create_reply(&element);
                    value.parse_response(buffer, 0);
                }
                self.map.set(key.str.copy_str(), value);
            }
            return;
        default:
            io::printfn("unknown tag: %c", self.tag);
            DString element = get_element_from_buffer(buffer);
            self.type = RedisReplyType.REDIS_REPLY_UNKNOWN;
            return;
    }
}

fn DString get_element_from_buffer(DString* buffer) {
    DString element = dstring::new();
    foreach(index, c: buffer.str_view()) {
        if (c == '\n') {
            break;
        }
        if (c != '\r') {
            element.append_char(c);
        }
    }
    chop_buffer(buffer);
    return element.copy();
}

fn void chop_buffer(DString* buffer) {
    foreach(index, c: buffer.str_view()) {
        if (c == '\n') {
            buffer.delete_range(0, index);
            break;
        }
    }
}

fn DString int_to_string(int value) {
    char[32] buffer;
    usz i = 0;

    // Handle negative numbers
    bool is_negative = value < 0;
    if (is_negative) {
        buffer[i++] = '-';
        value = -value;  // Convert value to positive for processing
    }

    // Convert the number to string (handle zero case)
    if (value == 0) {
        buffer[i++] = '0';
    } else {
        usz start = i;
        while (value > 0) {
            char digit = (char)(value % 10);  // Get last digit
            buffer[i++] = '0' + digit;
            value /= 10;
        }

        // Reverse integer part in buffer if not negative
        for (usz j = start; j < (i + start) / 2; j++) {
            char temp = buffer[j];
            buffer[j] = buffer[i - j + start - 1];
            buffer[i - j + start - 1] = temp;
        }
    }

    // Copy buffer to DString
    DString result = dstring::new_with_capacity(i + 1);
    for (usz k = 0; k < i; k++) {
        result.append_char(buffer[k]);
    }

    return result;
}

fn DString double_to_string(double value, int precision = 2) {
    char[64] buffer;
    usz i = 0;

    // Handle negative numbers
    if (value < 0) {
        buffer[i++] = '-';
        value = -value;
    }

    // Convert the integer part
    usz integer_part = (usz)value;
    double fractional_part = value - (double)integer_part;

    // Convert integer part to string
    if (integer_part == 0) {
        buffer[i++] = '0';
    } else {
        usz start = i;
        while (integer_part > 0) {
            buffer[i++] = '0' + (char)(integer_part % 10);
            integer_part /= 10;
        }
        // Reverse integer part in buffer
        for (usz j = start; j < (i + start) / 2; j++) {
            char temp = buffer[j];
            buffer[j] = buffer[i - j + start - 1];
            buffer[i - j + start - 1] = temp;
        }
    }

    // Decimal point
    buffer[i++] = '.';

    // Convert the fractional part
    for (int j = 0; j < precision; j++) {
        fractional_part *= 10;
        int fractional_digit = (int)fractional_part;
        buffer[i++] = '0' + (char)fractional_digit;
        fractional_part -= fractional_digit;
    }

    // Copy buffer to DString
    DString result = dstring::new_with_capacity(i + 1);
    for (usz k = 0; k < i; k++) {
        result.append_char(buffer[k]);
    }

    return result;
}
