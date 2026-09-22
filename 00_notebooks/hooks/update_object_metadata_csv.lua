lakefs = require("lakefs")
hook = require("hook")
strings = require("strings")
path = require("path")
table = require("table")

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
        
        -- If CSV file was added or changed
        if result.path_type == "object" and result.type ~= "removed" and strings.has_suffix(p.base_name, ".csv") then
            print("Read CSV file " .. result.path)
            -- Get CSV file
            code, object_content = lakefs.get_object(action.repository_id, action.source_ref, result.path)
            if code ~= 200 then
                error("could not fetch CSV file: HTTP " .. tostring(code) .. "body:\n" .. object_content)
            end
            
            object_lines = strings.split(object_content, "\r\n")
            column_header = strings.split(object_lines[1], ",")
                        
            for i = 2, #object_lines do
                column_values = strings.split(object_lines[i], ",")
                
                if column_values[2] then
                    metadata = {}
                    for j = 1, #column_values do
                        metadata[column_header[j]] = column_values[j]
                    end
                    object_to_edit = strings.replace(p.parent, args.annotation_folder_name, args.object_folder_name, 1) .. "n" .. column_values[1] .. "-" .. column_values[8] .. "/" .. column_values[2] .. "." .. args.object_file_name_extension
                    print("Update user metadata for " .. object_to_edit)
                    lakefs.update_object_user_metadata(action.repository_id, action.branch_id, object_to_edit, metadata)
                    print("")
                end
            end

        end
    end
    -- pagination
    has_more = resp.pagination.has_more
    after = resp.pagination.next_offset
end
