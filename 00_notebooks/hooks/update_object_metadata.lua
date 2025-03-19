lakefs = require("lakefs")
hook = require("hook")
json = require("encoding/json")
strings = require("strings")
path = require("path")
table = require("table")

-- main flow
after = ""
has_more = true

while has_more do
    local code, resp = lakefs.diff_refs(action.repository_id, action.commit.parents[1], action.branch_id, after, args.prefix)
    if code ~= 200 then
        error("could not diff: " .. resp.message)
    end
    for _, result in pairs(resp.results) do
        p = path.parse(result.path)
        if result.path_type == "object" and result.type ~= "removed" and strings.has_suffix(p.base_name, ".json") then
            print("Read json file " .. result.path)
            code, object_content = lakefs.get_object(action.repository_id, action.source_ref, result.path)
            if code ~= 200 then
                error("could not fetch json file: HTTP " .. tostring(code) .. "body:\n" .. object_content)
            end
            json_content = json.unmarshal(object_content)
            metadata = {}
            for k, v in pairs(json_content.annotation) do
                if type(v) == "table" then
                    for k1, v1 in pairs(v) do
                        if type(v1) == "table" then
                            for k2, v2 in pairs(v1) do
                                metadata[k .. "." .. k1 .. "." .. k2] = v2
                            end
                        else
                            metadata[k .. "." .. k1] = v1
                        end
                    end
                else
                    metadata[k] = v
                end
            end
            object_to_edit = string.sub(strings.replace(result.path,"Annotation_JSON","Images", 1),1,-5) .. "jpg"
            print("Update user metadata for " .. object_to_edit)
            
            lakefs.update_object_user_metadata(action.repository_id, action.branch_id, object_to_edit, metadata)
            print("")
        end
    end
    -- pagination
    has_more = resp.pagination.has_more
    after = resp.pagination.next_offset
end
