"""Live proof: capture a real Managed Agents session with stressless.

Creates (or reuses) a minimal Haiku agent + cloud environment, runs one
session with the event stream teed through stressless, then prints the run
row the collector stored. Costs well under $0.01.

Requires: ANTHROPIC_API_KEY with managed-agents beta access, DATABASE_URL
(or host config) pointing at a Postgres with the stressless schema.

    python examples/cma_live_proof.py
"""

from __future__ import annotations

import asyncio

import anthropic

from stressless import store, tee_session_stream
from stressless.cma import is_terminal_event, session_run_id

AGENT_NAME = "stressless-proof"
ENV_NAME = "stressless-proof-env"
MODEL = "claude-haiku-4-5"


async def _find_or_create(listing, create, name: str):
    page = await listing()
    for item in getattr(page, "data", None) or page:
        if getattr(item, "name", None) == name:
            return item
    return await create()


async def main() -> None:
    client = anthropic.AsyncAnthropic()

    environment = await _find_or_create(
        client.beta.environments.list,
        lambda: client.beta.environments.create(
            name=ENV_NAME,
            config={"type": "cloud", "networking": {"type": "unrestricted"}},
        ),
        ENV_NAME,
    )
    agent = await _find_or_create(
        client.beta.agents.list,
        lambda: client.beta.agents.create(
            name=AGENT_NAME,
            model=MODEL,
            system="Answer in one short sentence.",
        ),
        AGENT_NAME,
    )
    print(f"environment {environment.id} · agent {agent.id}")

    session = await client.beta.sessions.create(
        agent=agent.id, environment_id=environment.id, title="stressless cma proof"
    )
    print(f"session {session.id}")

    async with asyncio.timeout(240):
        # Stream-first, teed through stressless; then send the kickoff.
        stream = await client.beta.sessions.events.stream(session_id=session.id)
        await client.beta.sessions.events.send(
            session_id=session.id,
            events=[{
                "type": "user.message",
                "content": [{"type": "text", "text": "What is the capital of France?"}],
            }],
        )
        async for event in tee_session_stream(
            stream, kind="cma:proof", session_id=session.id, model=MODEL
        ):
            event_type = getattr(event, "type", "?")
            print(f"  event: {event_type}")
            if is_terminal_event(event):
                break

    await asyncio.sleep(1.5)  # let the fire-and-forget persist land
    pool = await store.get_pool()
    row = await pool.fetchrow(
        "SELECT agent_kind, status, model, num_turns, input_tokens, output_tokens,"
        " cache_read_tokens, cost_usd, tracecard FROM stressless.runs WHERE id = $1",
        session_run_id(session.id),
    )
    assert row is not None, "run row not stored"
    print("\nstored run:")
    for key, value in dict(row).items():
        print(f"  {key}: {value}")

    await client.beta.sessions.archive(session_id=session.id)
    print("\nsession archived · agent/environment kept for reuse")
    await store.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
