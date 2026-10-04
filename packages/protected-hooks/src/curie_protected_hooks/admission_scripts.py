"""Broker transactions, @spec PROTECTED-HOOK-ADMISSION-4/5.

Successful commands preceding a broker error persist. The final immutable
commit alone establishes acceptance.
"""

# @spec PROTECTED-HOOK-ADMISSION-4/5 PROTECTED-HOOK-LANE-2
TRANSACTION = r"""
-- @spec PROTECTED-HOOK-ADMISSION-4/5
local p = cjson.decode(ARGV[1])
local function answer(status, reason, receipt)
    -- @spec PROTECTED-HOOK-ADMISSION-3
    return cjson.encode({status=status,reason=reason or cjson.null,receipt=receipt or cjson.null})
end
local function greater(a,b)
    -- @spec PROTECTED-HOOK-ADMISSION-4
    if #a ~= #b then return #a > #b end
    return a > b
end
local function increment(a)
    -- @spec PROTECTED-HOOK-ADMISSION-4
    local out,carry = '',1
    for i=#a,1,-1 do
        local n=string.byte(a,i)-48+carry
        if n==10 then n=0;carry=1 else carry=0 end
        out=string.char(n+48)..out
    end
    if carry==1 then out='1'..out end
    return out
end
local function last_id()
    -- @spec PROTECTED-HOOK-ADMISSION-4/5
    if redis.call('TYPE',KEYS[8]).ok=='none' then return '0-0' end
    local fields=redis.call('XINFO','STREAM',KEYS[8])
    for i=1,#fields,2 do if fields[i]=='last-generated-id' then return fields[i+1] end end
    error('invalid stream metadata')
end
local function id_greater(a,b)
    -- @spec PROTECTED-HOOK-ADMISSION-4/5
    local am,as=string.match(a,'^(%d+)%-(%d+)$')
    local bm,bs=string.match(b,'^(%d+)%-(%d+)$')
    if am==bm then return greater(as,bs) end
    return greater(am,bm)
end
local expected={'string','string','string','string','string','zset','string','stream'}
for i=1,8 do
    local kind=redis.call('TYPE',KEYS[i]).ok
    if kind~='none' and kind~=expected[i] then error('invalid admission type') end
end
local info=redis.call('INFO','server')
local run=string.match(info,'run_id:([0-9a-f]+)')
if run~=p.run_id then return answer('refused','broker_identity_mismatch') end
local time=redis.call('TIME')
local now=tonumber(time[1])*1000+math.floor(tonumber(time[2])/1000)
local nowstr=string.format('%.0f',now)
-- Compare validated exact bytes, including missing records, before any write.
for _,v in ipairs(p.snapshots) do
    local raw=redis.call('GET',KEYS[v.index])
    local expectedraw=v.raw
    if expectedraw==cjson.null then expectedraw=false end
    if raw~=expectedraw then return 'retry' end
end
if p.mode=='read' then
    return cjson.encode(p.result)
end
local function authority()
    -- @spec PROTECTED-HOOK-ADMISSION-4/5 PROTECTED-HOOK-LANE-2
    if p.authority_reason~=cjson.null then return p.authority_reason end
    for _,v in ipairs(p.controls) do
        if redis.call('GET',KEYS[v.index])~=v.raw then return 'runtime_unavailable' end
    end
    if p.issued>now or now>=p.expires or p.expires-p.issued>p.max_readiness then
        return 'evidence_unavailable'
    end
    return nil
end
local function commit(intent,attempts)
    -- @spec PROTECTED-HOOK-ADMISSION-3/4/5
    local receipt={}
    for k,v in pairs(intent) do
        if k~='created_at_ms' and k~='deadline_ms'
           and k~='reserved_stream_id' and k~='envelope_sha256' then receipt[k]=v end
    end
    receipt.stream_id=intent.reserved_stream_id
    receipt.acceptance_status='accepted';receipt.tool_access='read-only'
    redis.call('DEL',KEYS[4])
    local state={schema_version=1,status='committed',recovery_attempts=attempts,
                 reason=cjson.null,receipt=receipt}
    if not redis.call('SET',KEYS[3],cjson.encode(state),'NX') then error('commit collision') end
    return answer('accepted',nil,receipt)
end
if p.mode=='new' then
    local reason=authority()
    if reason then return answer('refused',reason) end
    for i=1,4 do if redis.call('GET',KEYS[i]) then error('orphan admission') end end
    if redis.call('GET',KEYS[7]) then error('binding collision') end
    if redis.call('ZSCORE',KEYS[6],p.digest) then error('orphan quota') end
    if redis.call('ZCARD',KEYS[6])>=p.limit then return answer('refused','quota_full') end
    if now>9007199254440991 then error('deadline overflow') end
    local last=last_id()
    local ms,seq=string.match(last,'^(%d+)%-(%d+)$')
    local maximum='18446744073709551615'
    local reserved
    if greater(nowstr,ms) then reserved=nowstr..'-0'
    elseif seq~=maximum then reserved=ms..'-'..increment(seq)
    elseif ms~=maximum then reserved=increment(ms)..'-0'
    else error('stream exhausted') end
    local intent=p.intent
    intent.created_at_ms=nowstr;intent.deadline_ms=string.format('%.0f',now+300000)
    intent.reserved_stream_id=reserved
    if not redis.call('SET',KEYS[1],cjson.encode(intent),'NX') then error('intent collision') end
    redis.call('SET',KEYS[2],cjson.encode({schema_version=1,status='preparing',recovery_attempts=0,reason=cjson.null,receipt=cjson.null}))
    if redis.call('ZADD',KEYS[6],'NX',now,p.digest)~=1 then error('quota collision') end
    if not redis.call('SET',KEYS[7],ARGV[3],'NX') then error('binding collision') end
    if not redis.call('SET',KEYS[4],ARGV[2],'NX') then error('recovery collision') end
    redis.call('XADD',KEYS[8],reserved,'payload',ARGV[2],'protected_envelope',ARGV[3])
    return commit(intent,0)
end
local intent=p.intent
local attempts=p.attempts
local function fail(reason,count)
    -- @spec PROTECTED-HOOK-ADMISSION-5
    redis.call('DEL',KEYS[4]);redis.call('ZREM',KEYS[6],p.digest)
    redis.call('SET',KEYS[2],cjson.encode({schema_version=1,status='failed',recovery_attempts=count,reason=reason,receipt=cjson.null}))
    return answer('failed',reason)
end
if now>=tonumber(intent.deadline_ms) then return fail('deadline',attempts) end
if attempts>=10 then return fail('attempts_exhausted',attempts) end
attempts=attempts+1
local function pending()
    -- @spec PROTECTED-HOOK-ADMISSION-5
    if attempts>=10 then return fail('attempts_exhausted',attempts) end
    redis.call('SET',KEYS[2],cjson.encode({schema_version=1,status='preparing',recovery_attempts=attempts,reason=cjson.null,receipt=cjson.null}))
    return answer('preparing')
end
local reason=authority()
if reason then return pending() end
local entries=redis.call('XRANGE',KEYS[8],intent.reserved_stream_id,intent.reserved_stream_id)
if #entries>0 then
    local f=entries[1][2]
    if not p.entry or p.entry==cjson.null or #f~=4 then error('entry changed') end
    for i=1,4 do if f[i]~=p.entry[i] then error('entry changed') end end
elseif p.entry~=cjson.null then error('entry disappeared')
elseif not id_greater(intent.reserved_stream_id,last_id()) then
    return fail('stream_id_unappendable',attempts)
end
if not redis.call('ZSCORE',KEYS[6],p.digest)
   and redis.call('ZCARD',KEYS[6])>=p.limit then return pending() end
if #entries==0 and ARGV[2]=='' then return pending() end
redis.call('SET',KEYS[2],cjson.encode({schema_version=1,status='preparing',recovery_attempts=attempts,reason=cjson.null,receipt=cjson.null}))
if not redis.call('ZSCORE',KEYS[6],p.digest) then
    if redis.call('ZADD',KEYS[6],'NX',intent.created_at_ms,p.digest)~=1 then
        error('quota collision') end
end
if not redis.call('GET',KEYS[7]) then
    if not redis.call('SET',KEYS[7],ARGV[3],'NX') then error('binding collision') end
end
if #entries==0 then
    if not redis.call('GET',KEYS[4])
       and not redis.call('SET',KEYS[4],ARGV[2],'NX') then error('recovery collision') end
    redis.call('XADD',KEYS[8],intent.reserved_stream_id,'payload',ARGV[2],'protected_envelope',ARGV[3])
end
return commit(intent,attempts)
"""
