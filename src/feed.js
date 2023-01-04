import { mikser, onProcessed, useLogger, useOperations, constants, onLoaded } from 'mikser-core'
import { api, useMachineId } from './whitebox.js'
import Queue from 'queue'
import { v1 as uuidv1 } from 'uuid'
import aguid from 'aguid'
import { debounce } from 'throttle-debounce'
import _ from 'lodash'
import { stat } from 'fs/promises'

let queue = Queue({
    concurrency: 4,
    autostart: true
})

const clearCache = debounce(1000, async () => {
    const logger = useLogger()
    const { global } = mikser.config.whitebox
    logger.info('WhiteBox feed: %s %s', 'clear', 'cache')
    let data = {}
    if (!global) data.context = await useMachineId()
    return api('feed', '/api/catalog/clear/cache', data)
})

onLoaded(async () => {
    const logger = useLogger()
    if (mikser.options.clear) {
        const data = {}
        const { global } = mikser.config.whitebox
        if (global) {
            data.context = await useMachineId()
        }
        logger.info('WhiteBox feed: %s %s', 'clear', 'catalog')
        await api('feed', '/api/catalog/clear', data)
        clearCache()
    }
})

onProcessed(async () => {
    const logger = useLogger()
    const { global, services: { feed } } = mikser.config.whitebox || { services: {} }
    if (!feed) return

    const entitiesToAdd = useOperations([constants.OPERATION_CREATE, constants.OPERATION_UPDATE])
    .map(operation => operation.entity)
    .filter(entity => entity.type == 'document')

    for (let entity of entitiesToAdd) {
        logger.trace('WhiteBox feed: %s', entity.id)
        const data = {
            passportId: uuidv1(),
            vaultId: aguid(entity.id),
            refId: '/' + entity.name,
            type: 'mikser.' + entity.meta?.type || entity.type,
            data: _.pick(entity, ['meta', 'stamp', 'content', 'type', 'collection', 'format', 'id', 'uri']),
            date: await stat(entity.uri).mtime,
            vaults: entity.meta?.vaults,
        }
        if (!global) {
            data.context = await useMachineId()
            data.expire = feed.expire
        }

        queue.push(async () => {
            clearCache()
            logger.debug('WhiteBox feed: %s %s', 'keep', data.refId)
            await api('feed', '/api/catalog/keep/one', data)
        })
    }
    entitiesToAdd.length && logger.info('WhiteBox feed: %s %s', 'keep', entitiesToAdd.length)

    const entitiesToDelete = useOperations([constants.OPERATION_DELETE])
    .map(operation => operation.entity)
    .filter(entity => !entity.layout && entity.type == 'document')

    for (let entity of entitiesToDelete) {
        let data = {
            vaultId: aguid(entity.id),
        }
        if (!global) data.context = await useMachineId()
        if (!options.clear) {
            queue.push(() => {
                clearCache()
                logger.debug('WhiteBox feed: %s %s', 'remove', entity.id)
                return api('feed', '/api/catalog/remove', data)
            })
        }
    }
    entitiesToDelete.length && logger.info('WhiteBox feed: %s %s', 'remove', entitiesToDelete.length)
})