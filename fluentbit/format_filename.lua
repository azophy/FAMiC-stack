function format_filename(tag, timestamp, record)
    -- Extract container name from Docker log path
    local container_name = string.match(record["container_name"], "([^/]+)$")
    if container_name then
        record["container_name"] = container_name
    end
    return 2, timestamp, record
end

