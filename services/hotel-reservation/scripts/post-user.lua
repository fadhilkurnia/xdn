local url = ""

request = function()
    local method = "POST"
    local path   = url .. "/user?username=admin&password=123"
    local headers = {}
    return wrk.format(method, path, headers, nil)
end

