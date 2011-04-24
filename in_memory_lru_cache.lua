-- The LRU-Cache is a table which maintains the insertion order of
-- values with a doubled linked list. Each new value is placed at the front of
-- this list. So the oldest values are found at the back/tail of this list.
-- A 'value' is the real data, which is going to be cached.
-- Each 'value' is wrapped by a 'entry' object.
-- This 'entry' keeps the pointers for the doubled linked list and also the lifetime.
function lru_cache_create(maxsize, max_age)
    local lru_cache = {
        -- Number of current cache entries
        current_items = 0,
        -- Number of maximum cache entries
        maxsize = maxsize,
        -- Number of seconds how long a cache entry should be valid
        max_age = max_age,
        head = {key = "head", next_ = nil, prev_ = nil},
        tail = {key = "tail", next_ = nil, prev_ = nil},
        -- Stores the cache entries
        data = {}
    }
    -- head/tail are nodes in the linked list, which are never be dropped.
    -- They are sentinels which make the operations on the linked list easier
    lru_cache.head.prev_ = lru_cache.tail
    lru_cache.tail.next_ = lru_cache.head
    return lru_cache
end

function print_linked_list(cache)
    local cur = cache.tail
    while cur ~= nil do
        print(cur.key)
        cur = cur.next_
    end
end

function lru_cache_add_entry(cache, entry)
    cache.head.prev_.next_ = entry
    entry.prev_ = cache.head.prev_

    cache.head.prev_ = entry
    entry.next_ = cache.head

    cache.current_items = cache.current_items + 1
    cache.data[entry.key] = entry
end

function lru_cache_remove_entry(cache, entry)
    cache.current_items = cache.current_items - 1
    cache.data[entry.key] = nil

    entry.prev_.next_ = entry.next_
    entry.next_.prev_ = entry.prev_
end

function lru_cache_del_last_entry(cache)
    if cache.current_items ~= 0 then
        lru_cache_remove_entry(cache, cache.tail.next_)
    end
end

function lru_cache_set(cache, key, value)
    local entry = {
        -- If 'lru_cache_del_last_entry' is called the key is needed to remove
        -- the entry from cache.data
        key = key,
        value = value,
        max_age = os.time() + cache.max_age,
        next_ = nil,
        prev_ = nil
    }
    if cache.current_items > cache.maxsize then
        lru_cache_del_last_entry(cache)
    end
    lru_cache_add_entry(cache, entry)
end

function lru_cache_get(cache, key)
    local entry = cache.data[key]

    if entry == nil then
        return nil
    elseif entry.max_age > os.time() then
        -- Entry is there + young enough => Cache hit
        return entry.value
    else
        -- auto delete the entry because its too old to be kept
        lru_cache_remove_entry(cache, entry)
        return nil
    end
end

-- Create the global cache for the whole process
if not proxy.global.lru_cache then
    proxy.global.lru_cache = lru_cache_create(100000, 30)
end

function read_query( packet )
    -- Called by mysql-proxy for each query comming from a mysql-client
    if packet:byte() == proxy.COM_QUERY then
        local query = packet:sub(2)
        if query:sub(1,6):lower() == 'select' then
            local resultset = lru_cache_get(proxy.global.lru_cache, query)
            if resultset then
                proxy.response.type = proxy.MYSQLD_PACKET_OK
                proxy.response.resultset = resultset
                return proxy.PROXY_SEND_RESULT
            else
                -- Nothing valid found in the cache, change the proxy.queries
                -- variable in order to trigger the 'read_query_result' hook
                proxy.queries:append(1, packet, {resultset_is_needed = true})
                return proxy.PROXY_SEND_QUERY
            end
        end
    end
end

function deep_copy_result(resultset)
    -- Seems like the resultset from mysql-proxy is freed after the query ends.
    -- To have the data aviable next time, deep copy it
    local field_count = 1
    local fields = resultset.fields
    local deep_coypy = {rows={}, fields={}}

    while fields[field_count] do
        local field = fields[field_count]
        table.insert(deep_coypy.fields, {type=field.type, name=field.name} )
        field_count = field_count + 1
    end

    for row in resultset.rows do
        table.insert(deep_coypy.rows, row)
    end

    return deep_coypy
end

function read_query_result(result_packet)
    -- Called by mysql-proxy if the result from mysqldb is there.
    -- This only gets called if the proxy.queries queue is modified
    query = result_packet.query:sub(2)
    lru_cache_set(proxy.global.lru_cache, query, deep_copy_result(result_packet.resultset))
end
