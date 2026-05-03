return function(ctx)
    return ctx.tool_call("notion_api_request", {
        method = ctx.input.method,
        path = ctx.input.path,
        body_json = ctx.input.body_json
    })
end
