"""Atomic commissioning journal scripts, @spec PROTECTED-COMMISSIONING-3.

The scripts run under the commissioning control principal's ACL rules, which
grant ``GET``, ``SET``, ``TYPE``, ``TIME`` and ``INFO server`` and nothing that
deletes or expires a key. Every command a script issues is checked again
against those rules, so the scripts themselves are the compare and set
discipline over the control's plain ``SET`` right.

``KEYS`` are the permit, journal and per-runtime floor keys. A script reads the
issuer-written permit and compares it byte for byte with the caller's permit,
then takes the limits, time bounds, broker ``run_id`` and key names from those
stored bytes, never from separate arguments. Broker ``TIME`` and the live
``run_id`` are read inside the same atomic operation.

Counters and generations stay canonical decimal strings. Lua numbers are used
only for single digits and for milliseconds, which are below 2**53.
"""

# ruff: noqa: E501  Embedded Lua is kept readable as Lua source.

# Shared by every commissioning script that reads or advances a journal.
PRELUDE = r"""
local MAX = '9223372036854775807'
local COUNTERS = {'deliveries','launches','model_calls','output_tokens','spend_microusd'}
local STATES = {prepared=true, running=true, closing=true, closed=true, failed=true}
local function refuse(reason) return {'refused', reason} end
local function decimal(s, zero)
  if type(s) ~= 'string' or not string.match(s, '^[0-9]+$') then return false end
  if #s > 1 and string.sub(s, 1, 1) == '0' then return false end
  if s == '0' and not zero then return false end
  return #s < #MAX or (#s == #MAX and s <= MAX)
end
local function cmp(a, b)
  if #a ~= #b then return #a < #b and -1 or 1 end
  if a == b then return 0 end
  return a < b and -1 or 1
end
local function add(a, b)
  local i, j, carry, out = #a, #b, 0, ''
  while i > 0 or j > 0 or carry > 0 do
    local x = i > 0 and tonumber(string.sub(a, i, i)) or 0
    local y = j > 0 and tonumber(string.sub(b, j, j)) or 0
    local digit = x + y + carry
    out = tostring(digit % 10) .. out
    carry = math.floor(digit / 10)
    i = i - 1; j = j - 1
  end
  return out
end
local function closed(t, members)
  if type(t) ~= 'table' then return false end
  local count = 0
  for k, _ in pairs(t) do
    if type(k) ~= 'string' or not members[k] then return false end
    count = count + 1
  end
  local want = 0
  for _ in pairs(members) do want = want + 1 end
  return count == want
end

local permit_key, journal_key, floor_key = KEYS[1], KEYS[2], KEYS[3]
local verb, permit_bytes, permit_digest = ARGV[1], ARGV[2], ARGV[3]

-- The issuer-written permit, byte equal to the caller's validated permit.
local stored = redis.call('GET', permit_key)
if not stored or stored ~= permit_bytes then return refuse('binding') end
local ok, permit = pcall(cjson.decode, stored)
if not ok or type(permit) ~= 'table' or type(permit.limits) ~= 'table' or
   type(permit.broker_identity) ~= 'table' then return refuse('binding') end
local suffix = permit.campaign_id .. ':' .. permit.campaign_generation
if permit_key ~= 'protected:commissioning:permit:' .. suffix or
   journal_key ~= 'protected:commissioning:journal:' .. suffix or
   floor_key ~= 'protected:commissioning:journal:floor:' .. permit.runtime_id or
   type(permit_digest) ~= 'string' or not string.match(permit_digest, '^[0-9a-f]+$') or
   #permit_digest ~= 64 then return refuse('binding') end
local limits = {}
for i, name in ipairs(COUNTERS) do
  if not decimal(permit.limits[name], false) then return refuse('binding') end
  limits[i] = permit.limits[name]
end

-- Live authority: broker TIME inside the permit interval and the bound run_id.
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local live_run_id = string.match(redis.call('INFO', 'server'), 'run_id:(%x+)')
local function authority()
  if live_run_id ~= permit.broker_identity.run_id then return 'run_id' end
  if now < tonumber(permit.issued_at_ms) or now >= tonumber(permit.expires_at_ms) then return 'time' end
  return nil
end

local JOURNAL = {record_type=true, permit_digest=true, broker_run_id=true, state=true,
                 generation=true, counters=true, exhausted=true}
local COUNTER_SET = {}
for _, name in ipairs(COUNTERS) do COUNTER_SET[name] = true end
local function load_journal()
  local raw = redis.call('GET', journal_key)
  if not raw then return nil, 'absent' end
  local decoded, j = pcall(cjson.decode, raw)
  if not decoded or not closed(j, JOURNAL) or not closed(j.counters, COUNTER_SET) then return nil, 'corrupt' end
  if j.record_type ~= 'commissioning_journal' or j.permit_digest ~= permit_digest or
     j.broker_run_id ~= permit.broker_identity.run_id or type(j.state) ~= 'string' or
     not STATES[j.state] or not decimal(j.generation, false) then return nil, 'corrupt' end
  local exhausted = nil
  for i, name in ipairs(COUNTERS) do
    local used = j.counters[name]
    if not decimal(used, true) or cmp(used, limits[i]) > 0 then return nil, 'corrupt' end
    if j.exhausted == name then exhausted = i end
  end
  if j.exhausted ~= '' then
    if not exhausted or j.state == 'prepared' or j.state == 'running' or
       j.counters[j.exhausted] ~= limits[exhausted] then return nil, 'corrupt' end
  end
  return j, nil
end
local function save_journal(j)
  redis.call('SET', journal_key, cjson.encode(j))
end
local function journal_reply(j)
  local reply = {'journal', j.state, j.generation}
  for _, name in ipairs(COUNTERS) do table.insert(reply, j.counters[name]) end
  table.insert(reply, j.exhausted)
  table.insert(reply, string.format('%.0f', now))
  return reply
end
local function advance(j, target)
  local next_generation = add(j.generation, '1')
  if not decimal(next_generation, false) then return refuse('generation') end
  j.state = target
  j.generation = next_generation
  save_journal(j)
  return journal_reply(j)
end
"""

_JOURNAL_BODY = r"""
local expected = ARGV[4]
if verb == 'prepare' then
  local denied = authority()
  if denied then return refuse(denied) end
  if redis.call('GET', journal_key) then return refuse('exists') end
  local floor = redis.call('GET', floor_key)
  if not floor then floor = '0' end
  if not decimal(floor, true) then return refuse('corrupt') end
  if not decimal(permit.campaign_generation, false) or cmp(permit.campaign_generation, floor) <= 0 then
    return refuse('floor')
  end
  -- The floor moves before the journal exists, in the same atomic step.
  redis.call('SET', floor_key, permit.campaign_generation)
  local counters = {}
  for _, name in ipairs(COUNTERS) do counters[name] = '0' end
  local j = {record_type='commissioning_journal', permit_digest=permit_digest,
             broker_run_id=permit.broker_identity.run_id, state='prepared',
             generation='1', counters=counters, exhausted=''}
  save_journal(j)
  return journal_reply(j)
end
local j, problem = load_journal()
if not j then return refuse(problem) end
if verb == 'status' then return journal_reply(j) end
if verb == 'require_running' then
  if j.state ~= 'running' then return refuse('state') end
  local denied = authority()
  if denied then return refuse(denied) end
  return journal_reply(j)
end
local targets = {
  start = {prepared='running'},
  begin_close = {prepared='closing', running='closing'},
  confirm_closed = {closing='closed'},
  fail = {prepared='failed', running='failed', closing='failed'},
}
local target = targets[verb] and targets[verb][j.state]
if not targets[verb] then return refuse('verb') end
if not target then return refuse('state') end
if j.generation ~= expected then return refuse('generation') end
if verb == 'start' then
  local denied = authority()
  if denied then return refuse(denied) end
end
return advance(j, target)
"""

JOURNAL_SCRIPT = PRELUDE + _JOURNAL_BODY

# Reservations and launch ownership read the journal through the same prelude.
_RECORDS = r"""
local KINDS = {delivery=1, launch=2, model_call=3}
local RESERVATION = {record_type=true, permit_digest=true, operation_id=true, kind=true,
                     request_digest=true, charge=true, status=true, claim=true}
local function uuid(s)
  return type(s) == 'string' and #s == 36 and
    string.match(s, '^%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x$') ~= nil and
    string.lower(s) == s
end
local function sha256(s)
  return type(s) == 'string' and #s == 64 and string.match(s, '^[0-9a-f]+$') ~= nil
end
local function reservation_key(operation_id)
  return 'protected:commissioning:reservation:' .. suffix .. ':' .. operation_id
end
local function load_reservation(key)
  local raw = redis.call('GET', key)
  if not raw then return nil, 'absent' end
  local decoded, r = pcall(cjson.decode, raw)
  if not decoded or not closed(r, RESERVATION) or not closed(r.charge, COUNTER_SET) then return nil, 'corrupt' end
  if r.record_type ~= 'commissioning_reservation' or r.permit_digest ~= permit_digest or
     not uuid(r.operation_id) or key ~= reservation_key(r.operation_id) or not KINDS[r.kind] or
     not sha256(r.request_digest) or (r.status ~= 'reserved' and r.status ~= 'dispatched') or
     type(r.claim) ~= 'string' or (r.claim ~= '' and not uuid(r.claim)) then return nil, 'corrupt' end
  for _, name in ipairs(COUNTERS) do
    if not decimal(r.charge[name], true) then return nil, 'corrupt' end
  end
  return r, nil
end
local function reservation_reply(r, j, fresh)
  local reply = {'reservation', r.status, r.kind, r.request_digest}
  for _, name in ipairs(COUNTERS) do table.insert(reply, r.charge[name]) end
  for _, value in ipairs({r.claim, fresh and '1' or '0', j.state, j.generation, j.exhausted,
                          string.format('%.0f', now)}) do
    table.insert(reply, value)
  end
  return reply
end
"""

_RESERVATION_BODY = r"""
local reservation_key_name = KEYS[4]
local operation_id, kind, request_digest = ARGV[4], ARGV[5], ARGV[6]
if not uuid(operation_id) or reservation_key_name ~= reservation_key(operation_id) then return refuse('binding') end
local j, problem = load_journal()
if not j then return refuse(problem) end
local r
r, problem = load_reservation(reservation_key_name)
if problem == 'corrupt' then return refuse(problem) end
if verb == 'reserve' then
  if not KINDS[kind] or not sha256(request_digest) then return refuse('charge') end
  local charge, positive = {}, false
  for i, name in ipairs(COUNTERS) do
    if not decimal(ARGV[6 + i], true) then return refuse('charge') end
    charge[name] = ARGV[6 + i]
  end
  if charge[COUNTERS[KINDS[kind]]] == '0' then return refuse('charge') end
  if r then
    -- A replay returns the recorded status and never charges or authorizes again.
    if r.kind ~= kind or r.request_digest ~= request_digest then return refuse('mismatch') end
    for _, name in ipairs(COUNTERS) do
      if r.charge[name] ~= charge[name] then return refuse('mismatch') end
    end
    return reservation_reply(r, j, false)
  end
  if j.state ~= 'running' then return refuse('state') end
  local denied = authority()
  if denied then return refuse(denied) end
  local totals, exhausted = {}, nil
  for i, name in ipairs(COUNTERS) do
    local total = add(j.counters[name], charge[name])
    if not decimal(total, true) or cmp(total, limits[i]) > 0 then return refuse('limit') end
    totals[name] = total
    if total == limits[i] and not exhausted then exhausted = name end
  end
  j.counters = totals
  r = {record_type='commissioning_reservation', permit_digest=permit_digest,
       operation_id=operation_id, kind=kind, request_digest=request_digest,
       charge=charge, status='reserved', claim=''}
  redis.call('SET', reservation_key_name, cjson.encode(r))
  if exhausted then
    -- Reaching a limit closes the campaign in the same step; nothing dispatches after it.
    j.exhausted = exhausted
    local reply = advance(j, 'closing')
    if reply[1] ~= 'journal' then return reply end
  else
    save_journal(j)
  end
  return reservation_reply(r, j, true)
end
if not r then return refuse('absent') end
if verb == 'read' then return reservation_reply(r, j, false) end
if verb ~= 'dispatch' then return refuse('verb') end
-- Consumed before forwarding: a second dispatch of one ID is never authority.
if r.status == 'dispatched' then return reservation_reply(r, j, false) end
if j.state ~= 'running' then return refuse('state') end
local denied = authority()
if denied then return refuse(denied) end
r.status = 'dispatched'
redis.call('SET', reservation_key_name, cjson.encode(r))
return reservation_reply(r, j, true)
"""

RESERVATION_SCRIPT = PRELUDE + _RECORDS + _RESERVATION_BODY

_OWNERSHIP_BODY = r"""
local ownership_key, index_key = KEYS[4], KEYS[5]
local launch_id, expected, proposed_bytes = ARGV[4], ARGV[5], ARGV[6]
if not uuid(launch_id) or
   ownership_key ~= 'protected:commissioning:ownership:' .. suffix .. ':' .. launch_id or
   index_key ~= 'protected:commissioning:ownership:' .. suffix then return refuse('binding') end
local j, problem = load_journal()
if not j then return refuse(problem) end
local function decode_ownership(raw)
  local decoded, o = pcall(cjson.decode, raw)
  if not decoded or type(o) ~= 'table' or type(o.campaign) ~= 'table' or
     o.record_type ~= 'commissioning_launch_ownership' or o.launch_id ~= launch_id or
     not decimal(o.revision, false) then return nil end
  local c = o.campaign
  if c.campaign_id ~= permit.campaign_id or c.campaign_generation ~= permit.campaign_generation or
     c.permit_digest ~= permit_digest or c.bootstrap_digest ~= permit.bootstrap_digest or
     c.manifest_digest ~= permit.manifest_digest or c.runtime_id ~= permit.runtime_id or
     c.runtime_generation ~= permit.runtime_generation or
     c.broker_run_id ~= permit.broker_identity.run_id then return nil end
  return o
end
local function load_index()
  local raw = redis.call('GET', index_key)
  if not raw then return {} end
  local decoded, list = pcall(cjson.decode, raw)
  if not decoded or type(list) ~= 'table' then return nil end
  for _, id in pairs(list) do if not uuid(id) then return nil end end
  return list
end
local index = load_index()
if not index then return refuse('corrupt') end
if verb == 'list' then
  local reply = {'ownership_index'}
  for _, id in ipairs(index) do table.insert(reply, id) end
  return reply
end
local current = redis.call('GET', ownership_key)
if verb == 'read' then
  if not current then return {'ownership_absent'} end
  return {'ownership', current}
end
local proposed = decode_ownership(proposed_bytes)
if not proposed then return refuse('binding') end
if verb == 'create' then
  -- Ownership is recorded before any create, only under live authority and only
  -- for a launch reservation already consumed for dispatch and not yet claimed.
  if current then return refuse('exists') end
  if proposed.state ~= 'creating' or proposed.revision ~= '1' then return refuse('ownership') end
  if j.state ~= 'running' then return refuse('state') end
  local denied = authority()
  if denied then return refuse(denied) end
  local reservation_key_name = KEYS[6]
  local reservation_id = ARGV[7]
  if not uuid(reservation_id) or reservation_key_name ~= reservation_key(reservation_id) then return refuse('binding') end
  local r
  r, problem = load_reservation(reservation_key_name)
  if not r then return refuse(problem) end
  if r.kind ~= 'launch' or r.status ~= 'dispatched' or
     r.request_digest ~= proposed.launch_request_digest then return refuse('ownership') end
  if r.claim ~= '' then return refuse('claimed') end
  r.claim = launch_id
  redis.call('SET', reservation_key_name, cjson.encode(r))
  table.insert(index, launch_id)
  redis.call('SET', index_key, cjson.encode(index))
  redis.call('SET', ownership_key, proposed_bytes)
  return {'ownership', proposed_bytes}
end
if verb ~= 'advance' then return refuse('verb') end
-- Retirement and observation stay available after expiry, closing or a run_id change.
if not current then return refuse('absent') end
if current ~= expected then return refuse('generation') end
local before = decode_ownership(current)
if not before then return refuse('corrupt') end
if proposed.revision ~= add(before.revision, '1') then return refuse('generation') end
redis.call('SET', ownership_key, proposed_bytes)
return {'ownership', proposed_bytes}
"""

OWNERSHIP_SCRIPT = PRELUDE + _RECORDS + _OWNERSHIP_BODY
