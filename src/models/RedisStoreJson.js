import errors from '../lib/errors'
import * as redis from 'redis'
import 'regenerator-runtime/runtime'

function partsFromFilename (fname) {
  const body = fname.slice(0, -5)
  return body.split('__')
}

class RedisStoreJson {
  constructor(){
    this.redisClient = redis.createClient({
      url: process.env.REDIS_TLS_URL,
      socket: {
        tls: true,
        rejectUnauthorized: false
      },
      retry_strategy: function(options) {
        if (options.error && options.error.code === "ECONNREFUSED") {
          console.log('E 1')
          // End reconnecting on a specific error and flush all commands with
          // a individual error
          return new Error("The server refused the connection");
        }
        if (options.total_retry_time > 1000 * 60 * 60) {
          console.log('E 2')
          // End reconnecting after a specific timeout and flush all commands
          // with a individual error
          return new Error("Retry time exhausted");
        }
        if (options.attempt > 10) {
          console.log('E 3')
          // End reconnecting with built in error
          return undefined;
        }

        console.log('E 4')
        // reconnect after
        return Math.min(options.attempt * 100, 3000);
      }
    })

    this.redisClient.on('error', err => console.error('client error', err));
    this.redisClient.on('connect', () => console.log('client is connect'));
    this.redisClient.on('reconnecting', () => console.log('client is reconnecting'));
    this.redisClient.on('ready', () => console.log('client is ready'));
  
    this.redisClient.connect()
  }

  async index () {
    const keys = await this.redisClient.SMEMBERS('sheet-keys')

    const jsons = keys.filter(f => f.match(/.*\.json$/))
    const parts = jsons.map(partsFromFilename)

    const dataKeys = parts.map(p => `${p[0]}/${p[1]}/${p[2]}`)

    console.log(dataKeys)

    return dataKeys
  }

  async save (url, data) {
    const parts = url.split('/')
    const key = `${parts[0]}__${parts[1]}__${parts[2]}.json`

    await this.redisClient.SET(key, JSON.stringify(data))
    await this.redisClient.SADD('sheet-keys', key)
  }

  async load (url) {
    const parts = url.split('/')
    const key = `${parts[0]}__${parts[1]}__${parts[2]}.json`

    const keyExists = await this.redisClient.EXISTS(key)

    if(!keyExists) {
      return Promise.reject(errors.noResource(parts))
    }

    const value = await this.redisClient.GET(key)

    const data = JSON.parse(value)

    if (parts.length === 3) {  
      // No lookup if the requested url doesn't have a fragment
      return data
    } else if (parts[2] === 'ids') {
      // Do a lookup if fragment is included to filter a relevant item
      // When the resource requested is 'ids'
      const id = parseInt(parts[3])
      if (!isNaN(id) && id >= 0 && id < data.length) {
        return data[id]
      } else {
        throw errors.noFragment(parts)
      }
    } else {
      // Do a lookup if fragment is included to filter a relevant item
      const index = parseInt(parts[3])
      if (!isNaN(index) && index >= 0 && index < data.length) {
        return data.filter((vl, idx) => idx === index)[0]
      } else {
        throw errors.noFragment(parts)
      }
    }
  }
}

export default RedisStoreJson
