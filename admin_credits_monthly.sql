-- =============================================================================
-- AbimconStudio — Monthly credit auto-refresh schedule
-- -----------------------------------------------------------------------------
-- Depends on admin_schema.sql (run that first).
-- Uses pg_cron, which Supabase ships with but leaves disabled by default.
--
-- Run this in Supabase SQL Editor:
-- =============================================================================

-- 1. Enable pg_cron (idempotent).
create extension if not exists pg_cron with schema extensions;

-- 2. Remove any prior schedule with the same name so re-running is safe.
select cron.unschedule(jobid)
from cron.job
where jobname = 'abimcon_monthly_credit_refresh';

-- 3. Schedule the refresh: 00:30 UTC on the 1st of every month.
--    (Change '30 0 1 * *' to something else if your billing day isn't the 1st.)
select cron.schedule(
  'abimcon_monthly_credit_refresh',
  '30 0 1 * *',
  $$ select public.refresh_pro_credits(); $$
);

-- 4. Optional: signup-credit trigger.
--    When a new user row is inserted with plan_key='free', immediately grant
--    the 240 one-time credits via the helper RPC.
create or replace function public._on_new_user_profile() returns trigger
language plpgsql security definer as $$
begin
  if new.plan_key = 'free' then
    perform public.grant_signup_free_credits(new.email);
  end if;
  return new;
end;
$$;

drop trigger if exists trg_on_new_user_profile on public.user_profiles;
create trigger trg_on_new_user_profile
  after insert on public.user_profiles
  for each row execute function public._on_new_user_profile();

-- 5. Verify with:
--    select * from cron.job;
--    select * from cron.job_run_details order by end_time desc limit 10;
--
--    select public.refresh_pro_credits();  -- manual kick
--
-- =============================================================================
-- END admin_credits_monthly.sql
-- =============================================================================
