-- Allow a third sampling outcome, 'skipped', for benign, expected provider
-- conditions that are not failures (e.g. the orderbook is too shallow to
-- absorb the sampled tier notional on a Hyperliquid spot leg). These rows are
-- shown neutrally in the activity feed and excluded from the failures panel.
ALTER TABLE public.router_route_cost_sample_events
    DROP CONSTRAINT router_route_cost_sample_events_outcome_check;

ALTER TABLE public.router_route_cost_sample_events
    ADD CONSTRAINT router_route_cost_sample_events_outcome_check
        CHECK (outcome IN ('succeeded', 'failed', 'skipped'));
