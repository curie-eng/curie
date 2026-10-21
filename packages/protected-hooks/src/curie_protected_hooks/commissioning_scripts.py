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
