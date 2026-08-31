// =============================================================================
// AgentCore Policy (Cedar) for the autonomous curation agent.
//
// Enforcement model (provided by AgentCore Policy):
//   * default-deny  -> any tool not explicitly permitted is denied
//   * forbid-wins   -> an explicit forbid always beats any permit
//
// The deploy script substitutes the placeholders before creating the policies:
//   %%AGENT_PRINCIPAL%%  the agent runtime's IAM role ARN (AWS_IAM inbound auth)
//   %%GATEWAY_ARN%%      the Gateway ARN
//   %%TARGET%%           the Lambda target name; tool actions are "<target>___<tool>"
//
// What the agent is ALLOWED to do: exactly the ten curated capabilities below.
// What the agent is DENIED:
//   * merge_data_pull_request           -> explicit forbid (headline denial)
//   * every other lakeFS operation      -> not exposed as a tool + default-deny
//   * writes to baseline/source/main    -> no such tool exists; the only write
//                                          tool (write_workspace_object) is
//                                          pinned to the workspace output prefix
//                                          server-side
//   * repo/branch deletion, unrelated
//     repositories/prefixes, arbitrary
//     lakeFS API calls                  -> not exposed + default-deny
// =============================================================================

// ---- ALLOW: the curated capability set --------------------------------------
permit(
    principal == AgentCore::IamEntity::"%%AGENT_PRINCIPAL%%",
    action in [
        AgentCore::Action::"%%TARGET%%___get_demo_context",
        AgentCore::Action::"%%TARGET%%___list_demo_objects",
        AgentCore::Action::"%%TARGET%%___read_demo_object",
        AgentCore::Action::"%%TARGET%%___plan_curation",
        AgentCore::Action::"%%TARGET%%___write_workspace_object",
        AgentCore::Action::"%%TARGET%%___update_workspace_metadata",
        AgentCore::Action::"%%TARGET%%___commit_workspace",
        AgentCore::Action::"%%TARGET%%___diff_workspace",
        AgentCore::Action::"%%TARGET%%___validate_workspace",
        AgentCore::Action::"%%TARGET%%___create_data_pull_request",
        AgentCore::Action::"%%TARGET%%___get_data_pull_request"
    ],
    resource == AgentCore::Gateway::"%%GATEWAY_ARN%%"
);

// ---- DENY: merging is never permitted for the agent identity ----------------
// forbid-wins guarantees this beats any permit. A human merges via the local
// approval tool, which calls lakeFS Cloud directly (not through this Gateway).
forbid(
    principal,
    action == AgentCore::Action::"%%TARGET%%___merge_data_pull_request",
    resource == AgentCore::Gateway::"%%GATEWAY_ARN%%"
);
