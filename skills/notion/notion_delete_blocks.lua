return function(ctx)
    return ctx.tool_call("notion_delete_block", {
        block_refs = ctx.input.block_refs
    })
end
