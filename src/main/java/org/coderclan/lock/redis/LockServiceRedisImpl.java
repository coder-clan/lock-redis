package org.coderclan.lock.redis;

import org.coderclan.lock.LockBean;
import org.coderclan.lock.LockException;
import org.coderclan.lock.LockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;

import java.util.Objects;

/**
 * LockService Redis implement.
 * Created by aray.chou.cn(at)gmail(dot)com on 8/15/2018.
 */
public class LockServiceRedisImpl implements LockService {
    private static final Logger log = LoggerFactory.getLogger(LockServiceRedisImpl.class);

    @Autowired
    private RedisTemplate redisTemplate;

    @Override
    public boolean lock(LockBean lock) throws LockException {
        RedisConnection con = null;
        try {
            byte[] key = getKey(lock);
            byte[] value = redisTemplate.getKeySerializer().serialize(lock.getOwner());

            con = redisTemplate.getConnectionFactory().getConnection();

            for (int i = 1; i <= lock.getRetryTime() + 1; i++) {
                Boolean r = con.set(key, value, Expiration.seconds(lock.getMaxLockTime()), RedisStringCommands.SetOption.SET_IF_ABSENT);
                if (Boolean.TRUE.equals(r)) {
                    log.info("Lock successfully, LockType={},LockKey={},tried={}", lock.getLockType(), lock.getLockKey(), i);
                    return true;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Try to lock failed at {} time.", i);
                }
                Thread.sleep(lock.getRetryDelay());
            }

        } catch (Exception e) {
            String msg = "Try to lock but failed. LockType=" + lock.getLockType() + ",LockKey=" + lock.getLockKey();
            throw new LockException(msg, e);
        } finally {
            if (con != null) {
                con.close();
            }
        }
        log.info("Lock failed, LockType={},LockKey={}", lock.getLockType(), lock.getLockKey());
        return false;
    }

    private byte[] getKey(LockBean lock) {
        return redisTemplate.getKeySerializer().serialize(lock.getLockType() + lock.getLockKey());
    }

    @Override
    public void unlock(LockBean lock) throws LockException {
        RedisConnection con = null;
        try {
            byte[] key = getKey(lock);
            byte[] owner = redisTemplate.getKeySerializer().serialize(lock.getOwner());

            con = redisTemplate.getConnectionFactory().getConnection();
            byte[] ownerInRedis = con.get(key);

            if (ownerInRedis == null) {
                log.warn("Try to unlock but the lock not exists in redis, LockType={},LockKey={}", lock.getLockType(), lock.getLockKey());
                return;
            }

            if (Objects.deepEquals(ownerInRedis, owner)) {

                Long count = con.del(key);

                if (count != null && count == 0) {
                    log.warn("Try to unlock but the lock not exists in redis, LockType={},LockKey={}, deleted={}", lock.getLockType(), lock.getLockKey(), count);
                } else {
                    log.info("Unlock successfully, LockType={},LockKey={}, deleted={}", lock.getLockType(), lock.getLockKey(), count);
                }

            } else {
                // lock operation failed(exception countered or already obtained by other) but called unlock;
                // or the key in redis just expired and someone obtained at once
                Object realOwner = redisTemplate.getKeySerializer().deserialize(ownerInRedis);
                log.warn("Don't unlock since the lock owned by another owner, LockType={},LockKey={},owner={}, realOwner={}", lock.getLockType(), lock.getLockKey(), lock.getOwner(), realOwner);
            }

        } catch (Exception e) {
            String msg = "Try to unlock but failed. LockType=" + lock.getLockType() + ",LockKey=" + lock.getLockType();
            throw new LockException(msg, e);
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }
}
