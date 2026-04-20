return function(ctx)
    return ctx.tool_call("notion_fetch", {
        ref = ctx.input.ref,
        object_kind = ctx.input.object_kind,
        include_content = ctx.input.include_content,
        page_size = ctx.input.page_size
    })
end
