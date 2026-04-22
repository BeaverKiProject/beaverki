return function(ctx)
    return ctx.tool_call("notion_update_page", {
        ref = ctx.input.ref,
        properties_json = ctx.input.properties_json
    })
end
