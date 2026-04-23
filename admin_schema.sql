-- =============================================================================
-- AbimconStudio Admin Panel — Schema Additions (H-15.36+)
-- Apply in Supabase SQL Editor on project: zlpkqyxenzexbfnnvaor
-- Idempotent (create-if-not-exists + ON CONFLICT) so you can re-run safely.
-- =============================================================================
--
-- TABLES ADDED:
--   public.license_roles   — Abimcon_Student / Abimcon_Engineer / Abimcon_Pro Max
--                            with editable per-role limits (boq, downloads, ai)
--   public.plans           — FREE (240 one-time), PRO 9.9/39.9/99.9 monthly tiers
--   public.user_profiles   — extends the user identity with role + plan + geo
--   public.credit_balances — current monthly credit snapshot per user
--   public.credits_ledger  — append-only audit of every credit grant/spend
--   public.usage_events    — per-action log (boq_input, model_download, ai_*)
--   public.geoip_logs      — raw country/city hits (used for the map)
--   public.app_settings    — key/value config the admin panel can edit live
--
-- ROLES referenced:
--   anon       — the SketchUp extension (posts events, reads own profile)
--   authenticated — signed-in user in the admin dashboard
--   service_role  — admin dashboard when using the service-role JWT
--
-- All tables have Row-Level Security enabled. See the RLS block at the end.
-- =============================================================================

------------------------------------------------------------------------------
-- 1. license_roles — the 3 new roles, with editable per-role limits
------------------------------------------------------------------------------
create table if not exists public.license_roles (
  key                     text primary key,            -- abimcon_student | abimcon_engineer | abimcon_pro_max
  label                   text not null,               -- 'Abimcon_Student' etc (display name)
  max_boq_items           integer,                     -- NULL = unlimited
  max_downloads_per_month integer,                     -- NULL = unlimited
  max_ai_monthly_credits  integer,                     -- NULL = unlimited (rides the plan)
  notes                   text,
  active                  boolean not null default true,
  created_at              timestamptz not null default now(),
  updated_at              timestamptz not null default now()
);

-- Seed the 3 roles per spec:
--   Abimcon_Pro Max   -> ALL unlimited
--   Abimcon_Engineer  -> ALL + download cap 10,000/mo
--   Abimcon_Student   -> ALL + download cap 1,000/mo
insert into public.license_roles (key, label, max_boq_items, max_downloads_per_month, max_ai_monthly_credits, notes)
values
  ('abimcon_pro_max',  'Abimcon_Pro Max',  null, null, null, 'All features unlimited'),
  ('abimcon_engineer', 'Abimcon_Engineer', null, 10000, null, 'All features, 10000 downloads/month'),
  ('abimcon_student',  'Abimcon_Student',  null, 1000,  null, 'All features, 1000 downloads/month')
on conflict (key) do nothing;

------------------------------------------------------------------------------
-- 2. plans — Free + PRO tiers + TRIAL
------------------------------------------------------------------------------
create table if not exists public.plans (
  key              text primary key,  -- free | trial | pro_9_9 | pro_39_9 | pro_99_9
  label            text not null,
  price_usd        numeric(10,2) not null default 0,
  monthly_credits  integer not null default 0,
  one_time         boolean not null default false,  -- FREE is one-time-only
  active           boolean not null default true,
  description      text,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);

insert into public.plans (key, label, price_usd, monthly_credits, one_time, description)
values
  ('free',     'Free',            0.00,  240,   true,  '240 credits granted once on signup'),
  ('trial',    'Trial',           0.00,  500,   false, 'Monthly trial credits'),
  ('pro_9_9',  'PRO $9.9/mo',     9.90,  3000,  false, 'Monthly Pro tier 1'),
  ('pro_39_9', 'PRO $39.9/mo',   39.90,  9000,  false, 'Monthly Pro tier 2'),
  ('pro_99_9', 'PRO $99.9/mo',   99.90, 30000,  false, 'Monthly Pro tier 3')
on conflict (key) do nothing;

------------------------------------------------------------------------------
-- 3. user_profiles — identity + role + plan + last-known geo
--    Keyed by gmail to match the existing credentials listing.
------------------------------------------------------------------------------
create table if not exists public.user_profiles (
  email            text primary key,
  license_key      text unique,                        -- ABIM-4K9L-MN2P-X001 etc
  admin_role       text not null default 'viewer'      -- admin | editor | viewer
                   check (admin_role in ('admin','editor','viewer')),
  license_role     text references public.license_roles(key) on update cascade,
                                                       -- abimcon_student|engineer|pro_max
  plan_key         text references public.plans(key) on update cascade
                   default 'free',
  plan_renews_at   date,                               -- NULL for free/trial
  plan_started_at  timestamptz default now(),
  country          text,
  country_code     char(2),
  city             text,
  lat              numeric(9,6),
  lon              numeric(9,6),
  ip_hash          text,                               -- SHA256 of last-seen IP (no raw IP stored)
  last_seen_at     timestamptz,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);

create index if not exists user_profiles_country_idx on public.user_profiles (country_code);
create index if not exists user_profiles_plan_idx    on public.user_profiles (plan_key);
create index if not exists user_profiles_role_idx    on public.user_profiles (license_role);

------------------------------------------------------------------------------
-- 4. credits_ledger — append-only audit of every credit change
--    balance_after is computed in the trigger below so the ledger is canonical.
------------------------------------------------------------------------------
create table if not exists public.credits_ledger (
  id            bigserial primary key,
  user_email    text not null references public.user_profiles(email) on update cascade,
  delta         integer not null,                     -- +grant / -spend
  reason        text not null,                        -- 'signup_free' | 'monthly_pro_9_9' | 'ai_spend' | 'admin_adjust' | ...
  meta          jsonb,
  balance_after integer not null,
  created_at    timestamptz not null default now()
);

create index if not exists credits_ledger_user_idx on public.credits_ledger (user_email, created_at desc);

------------------------------------------------------------------------------
-- 5. credit_balances — current snapshot (fast lookup without scanning ledger)
--    Maintained by trigger on credits_ledger.
------------------------------------------------------------------------------
create table if not exists public.credit_balances (
  user_email       text primary key references public.user_profiles(email) on update cascade,
  balance          integer not null default 0,
  plan_key         text references public.plans(key) on update cascade,
  last_grant_at    timestamptz,                       -- when we last auto-refreshed
  last_grant_kind  text,                              -- 'signup_free' | 'monthly_pro_X' | 'admin_adjust'
  updated_at       timestamptz not null default now()
);

------------------------------------------------------------------------------
-- 6. usage_events — every tracked action (BOQ input, model download, AI call)
------------------------------------------------------------------------------
create table if not exists public.usage_events (
  id            bigserial primary key,
  user_email    text references public.user_profiles(email) on update cascade,
  kind          text not null
                check (kind in ('boq_input','model_download','ai_assistant','login','other')),
  qty           integer not null default 1,
  ai_credit_cost integer default 0,                   -- for ai_assistant events
  meta          jsonb,
  ip_hash       text,
  country       text,
  country_code  char(2),
  city          text,
  lat           numeric(9,6),
  lon           numeric(9,6),
  created_at    timestamptz not null default now()
);

create index if not exists usage_events_user_idx    on public.usage_events (user_email, created_at desc);
create index if not exists usage_events_kind_idx    on public.usage_events (kind, created_at desc);
create index if not exists usage_events_country_idx on public.usage_events (country_code);

------------------------------------------------------------------------------
-- 7. geoip_logs — raw country/city hits for the map (append-only)
--    Keep this narrow so the map query stays fast. usage_events also carries
--    geo, but geoip_logs is the canonical pin source.
------------------------------------------------------------------------------
create table if not exists public.geoip_logs (
  id            bigserial primary key,
  user_email    text,
  ip_hash       text,
  country       text,
  country_code  char(2),
  city          text,
  lat           numeric(9,6),
  lon           numeric(9,6),
  source        text default 'extension',
  created_at    timestamptz not null default now()
);

create index if not exists geoip_logs_country_idx on public.geoip_logs (country_code, created_at desc);
create index if not exists geoip_logs_city_idx    on public.geoip_logs (country_code, city);

------------------------------------------------------------------------------
-- 8. app_settings — editable config the admin dashboard can PATCH live
--    Single JSON blob per key; admin panel loads/saves these with the
--    service-role key. Anon/authenticated users can READ active flags only.
------------------------------------------------------------------------------
create table if not exists public.app_settings (
  key          text primary key,
  value        jsonb not null,
  description  text,
  updated_at   timestamptz not null default now(),
  updated_by   text
);

insert into public.app_settings (key, value, description) values
  ('feature.ai_assistant_enabled', 'true'::jsonb, 'Kill switch for the free AI assistant'),
  ('feature.model_downloads_enabled', 'true'::jsonb, 'Kill switch for model downloads'),
  ('pricing.pro_tiers', jsonb_build_array(
      jsonb_build_object('key','pro_9_9',  'price_usd',  9.90, 'credits',  3000),
      jsonb_build_object('key','pro_39_9', 'price_usd', 39.90, 'credits',  9000),
      jsonb_build_object('key','pro_99_9', 'price_usd', 99.90, 'credits', 30000)
   ), 'PRO plan tiers mirrored to plans table; edited here for convenience'),
  ('pricing.free_grant', jsonb_build_object('credits', 240, 'one_time', true),
      'Free plan signup grant'),
  ('limits.default_roles', jsonb_build_object(
      'abimcon_pro_max',  jsonb_build_object('downloads_per_month', null),
      'abimcon_engineer', jsonb_build_object('downloads_per_month', 10000),
      'abimcon_student',  jsonb_build_object('downloads_per_month', 1000)
   ), 'Mirror of license_roles limits; UI edits write to both tables')
on conflict (key) do nothing;

------------------------------------------------------------------------------
-- 9. updated_at trigger helper (used by every mutable table)
------------------------------------------------------------------------------
create or replace function public.touch_updated_at() returns trigger
language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_touch_license_roles  on public.license_roles;
drop trigger if exists trg_touch_plans          on public.plans;
drop trigger if exists trg_touch_user_profiles  on public.user_profiles;
drop trigger if exists trg_touch_credit_bal     on public.credit_balances;
drop trigger if exists trg_touch_app_settings   on public.app_settings;

create trigger trg_touch_license_roles  before update on public.license_roles  for each row execute function public.touch_updated_at();
create trigger trg_touch_plans          before update on public.plans          for each row execute function public.touch_updated_at();
create trigger trg_touch_user_profiles  before update on public.user_profiles  for each row execute function public.touch_updated_at();
create trigger trg_touch_credit_bal     before update on public.credit_balances for each row execute function public.touch_updated_at();
create trigger trg_touch_app_settings   before update on public.app_settings   for each row execute function public.touch_updated_at();

------------------------------------------------------------------------------
-- 10. credits_ledger trigger — maintain credit_balances in sync
------------------------------------------------------------------------------
create or replace function public.sync_credit_balance() returns trigger
language plpgsql as $$
declare
  new_bal integer;
begin
  -- running balance for this user after applying delta
  select coalesce(sum(delta), 0) + new.delta into new_bal
  from public.credits_ledger
  where user_email = new.user_email;

  new.balance_after = new_bal;

  insert into public.credit_balances(user_email, balance, last_grant_at, last_grant_kind)
  values (new.user_email, new_bal, now(),
          case when new.delta > 0 then new.reason else null end)
  on conflict (user_email) do update set
    balance         = new_bal,
    last_grant_at   = case when new.delta > 0 then now() else credit_balances.last_grant_at end,
    last_grant_kind = case when new.delta > 0 then new.reason else credit_balances.last_grant_kind end,
    updated_at      = now();

  return new;
end;
$$;

drop trigger if exists trg_sync_credit_balance on public.credits_ledger;
create trigger trg_sync_credit_balance
  before insert on public.credits_ledger
  for each row execute function public.sync_credit_balance();

------------------------------------------------------------------------------
-- 11. Helper RPCs — called from the extension / admin dashboard
------------------------------------------------------------------------------

-- Grant the one-time Free signup credit (no-op if the user already has a
-- 'signup_free' ledger entry).
create or replace function public.grant_signup_free_credits(p_email text)
returns void
language plpgsql security definer as $$
declare
  already boolean;
begin
  select exists(
    select 1 from public.credits_ledger
    where user_email = p_email and reason = 'signup_free'
  ) into already;
  if already then return; end if;

  insert into public.credits_ledger (user_email, delta, reason, meta)
  values (p_email, 240, 'signup_free', jsonb_build_object('plan','free'));
end;
$$;

-- Monthly auto-refresh for Pro subscribers. Called by pg_cron; also exposed
-- as an RPC so the admin dashboard can 'kick' a refresh by hand.
create or replace function public.refresh_pro_credits()
returns integer
language plpgsql security definer as $$
declare
  u record;
  granted integer := 0;
  credits integer;
begin
  for u in
    select up.email, up.plan_key, p.monthly_credits
    from public.user_profiles up
    join public.plans p on p.key = up.plan_key
    where p.key in ('pro_9_9','pro_39_9','pro_99_9','trial')
      and p.active
      and (
        not exists(
          select 1 from public.credits_ledger l
          where l.user_email = up.email
            and l.reason = 'monthly_' || up.plan_key
            and l.created_at >= date_trunc('month', now())
        )
      )
  loop
    credits := coalesce(u.monthly_credits, 0);
    if credits > 0 then
      insert into public.credits_ledger (user_email, delta, reason, meta)
      values (u.email, credits, 'monthly_' || u.plan_key,
              jsonb_build_object('plan', u.plan_key, 'period', to_char(now(),'YYYY-MM')));
      granted := granted + 1;
    end if;
  end loop;

  return granted;
end;
$$;

-- Convenience view for the admin dashboard map query.
create or replace view public.v_admin_user_map as
  select
    up.email,
    up.country,
    up.country_code,
    up.city,
    up.lat,
    up.lon,
    up.plan_key,
    up.license_role,
    up.last_seen_at,
    cb.balance as credits_balance
  from public.user_profiles up
  left join public.credit_balances cb on cb.user_email = up.email
  where up.lat is not null and up.lon is not null;

-- Summary-count view: users-per-country and users-per-city.
create or replace view public.v_admin_geo_summary as
  select country_code, country, city, count(*) as users
  from public.user_profiles
  where country is not null
  group by country_code, country, city
  order by users desc;

------------------------------------------------------------------------------
-- 12. RLS — lock everything down by default, carve anon-safe holes.
------------------------------------------------------------------------------
alter table public.license_roles   enable row level security;
alter table public.plans           enable row level security;
alter table public.user_profiles   enable row level security;
alter table public.credit_balances enable row level security;
alter table public.credits_ledger  enable row level security;
alter table public.usage_events    enable row level security;
alter table public.geoip_logs      enable row level security;
alter table public.app_settings    enable row level security;

-- anon (the extension) can READ the public pricing/role catalogs:
drop policy if exists "license_roles anon read" on public.license_roles;
create policy "license_roles anon read" on public.license_roles for select using (active);
drop policy if exists "plans anon read"        on public.plans;
create policy "plans anon read"        on public.plans        for select using (active);
drop policy if exists "app_settings anon read" on public.app_settings;
create policy "app_settings anon read" on public.app_settings for select using (true);

-- anon (the extension) can INSERT its own events + geo logs:
drop policy if exists "usage_events anon insert" on public.usage_events;
create policy "usage_events anon insert" on public.usage_events for insert with check (true);
drop policy if exists "geoip_logs anon insert"   on public.geoip_logs;
create policy "geoip_logs anon insert"   on public.geoip_logs   for insert with check (true);

-- Everything else is service-role-only (admin dashboard with the service key).
-- service_role bypasses RLS by design, so no extra policies needed.

-- =============================================================================
-- END admin_schema.sql
-- =============================================================================
