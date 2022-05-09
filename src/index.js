import http from 'http'
import path from 'path'
import express from 'express'
import initialize from './initialize'
import middleware from './middleware'
import api from './api'
import dotenv from 'dotenv'
import RedisStoreJson from './models/RedisStoreJson'
import cors from 'cors'
import e from 'express'

const hbs = require('express-handlebars')

dotenv.config()

let app = express()
app.server = http.createServer(app)
app.engine('.hbs', hbs({
  extname: '.hbs',
  defaultLayout: 'default'
}))
app.set('view engine', '.hbs')

console.log('env', process.env.NODE_ENV)

// enable cross origin requests explicitly in development
if (process.env.NODE_ENV === 'development') {
  console.log('Enabling CORS in development...')
  app.use(cors())
} else {
  console.log('Enabling CORS from ')
  app.use(cors({origin: 'https://timemap.pages.dev'}))
}

const config = process.env

initialize(RedisStoreJson, controller => {
  app.use(
    middleware({
      config,
      controller
    })
  )
  app.use(
    '/api',
    api({
      config,
      controller
    })
  )

  app.use(express.static(path.join(__dirname, 'public')))

  app.server.listen(process.env.PORT || 4040, () => {
    console.log('===========================================')
    console.log(`Server running on port ${app.server.address().port}`)
  })
})

export default app
