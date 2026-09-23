lakefs = require("lakefs")
hook = require("hook")
json = require("encoding/json")
strings = require("strings")
path = require("path")
table = require("table")

-- This function prints table values (key & value)
local function printTable(t)
    for k, v in pairs(t) do
        if type(v) == "table" then
            printTable(v) -- Recursion for nested tables
        else
            print(k, v)
        end
    end
end

local function parseMetadata(concat_key,t)
    for k, v in pairs(t) do
        -- concat nested keys
        if concat_key == "" then
            next_concat_key = k
        else
            next_concat_key = concat_key .. "." .. k
        end
        
        -- If nested JSON or table value then parse it further
        if type(v) == "table" then
            parseMetadata(next_concat_key,v) -- Recursion for nested tables
        else
            --print(next_concat_key, v)
            metadata[next_concat_key] = tostring(v)
        end
    end
end

-- main flow
after = ""
has_more = true

while has_more do
    -- run the diff
    local code, resp = lakefs.diff_refs(action.repository_id, action.commit.parents[1], action.branch_id, after, args.prefix)
    if code ~= 200 then
        error("could not diff: " .. resp.message)
    end
    for _, result in pairs(resp.results) do
        -- Parse object path in lakeFS
        p = path.parse(result.path)
        --printTable(p)
        -- If JSON file was added or changed
        if result.path_type == "object" and result.type ~= "removed" and strings.has_suffix(p.base_name, ".json") then
            print("Read json file " .. result.path)
            -- Get JSON file
            code, object_content = lakefs.get_object(action.repository_id, action.source_ref, result.path)
            if code ~= 200 then
                error("could not fetch json file: HTTP " .. tostring(code) .. "body:\n" .. object_content)
            end
            json_content = json.unmarshal(object_content)
            metadata = {}  -- reset per file
            parseMetadata("",json_content)
            
            object_to_edit = strings.replace(p.parent, args.annotation_folder_name, args.object_folder_name, 1) .. json_content.annotation.filename .. "." .. args.object_file_name_extension
            print("Update user metadata for " .. object_to_edit)
            lakefs.update_object_user_metadata(action.repository_id, action.branch_id, object_to_edit, metadata)
            print("")
        end
    end
    -- pagination
    has_more = resp.pagination.has_more
    after = resp.pagination.next_offset
end
