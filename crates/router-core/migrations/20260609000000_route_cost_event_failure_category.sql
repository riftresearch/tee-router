-- Add a coarse failure classification to the route-cost sampling activity feed.
-- Populated only for failed samples (rate_limited / timeout / no_route /
-- upstream_server / upstream_client / other); NULL for successes. Additive and
-- nullable so it is safe to apply to existing deployments. Powers the admin
-- dashboard failures log and is derived from the provider error string at write
-- time (see `classify_failure` in router-core).
ALTER TABLE public.router_route_cost_sample_events
    ADD COLUMN failure_category text;
