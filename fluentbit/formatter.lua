-- based on https://stackoverflow.com/a/73000423

-- read container_id from filepath field and add it as a new field
function get_container_id(tag, timestamp, record)
  path = record["filepath"]
  -- s = "./var/lib/docker/containers/a3118c5d7ff06b70100f0aee279b4811804453971bebad127a689e5cc5c8d7d8/a3118c5d7ff06b70100f0aee279b4811804453971bebad127a689e5cc5c8d7d8-json.log"
  container = {}
  for s in string.gmatch(path, "([^/]*)/") do
    table.insert(container, s)
  end
  record["container_id"] = container[6]
  return 2, timestamp, record
end

-- extract container_name from container's config file by regex and add it as a new field
-- this if useful for us to apply different filters to different container logs
function get_container_name(tag, timestamp, record)
  id = record["container_id"]
  file = "./var/lib/docker/containers/" .. id .. "/config.v2.json"
  if not file_exists(file) then return {} end
  local lines = ""
  for line in io.lines(file) do
    lines = lines .. line
  end

  pattern="\"LogPath\":\"[^\"]*\",\"Name\":\"[/]?([^\"]+)\""
  record["container_name"] = string.match(lines, pattern)
  return 2, timestamp, record
end

-- tell if a file exists on file system
function file_exists(file)
  local f = io.open(file, "rb")
  if f then f:close() end
  return f ~= nil
end

