return function(ctx)
    return ctx.tool_call("notion_append_block_children", {
        parent_ref = ctx.input.parent_ref,
        content = ctx.input.content,
        position = ctx.input.position,
        after_block_ref = ctx.input.after_block_ref
    })
end
