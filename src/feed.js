import Queue from 'queue'
import { v1 as uuidv1 } from 'uuid'
import aguid from 'aguid'
import { debounce } from 'throttle-debounce'
import _ from 'lodash'

export default ({ 
    mikser, 
    onProcessed, 
    useLogger, 
    useJournal, 
    onLoaded, 
    whiteboxApi, 
    useMachineId, 
    constants: { OPERATION }, 
}) => {
    let queue = Queue({
        concurrency: 4,
        autostart: true
    })
    queue.on('end', () => {
        const logger = useLogger()
        logger.info('WhiteBox feed completed')
    })
    
    const clearCache = debounce(1000, async () => {
        const logger = useLogger()
        logger.info('WhiteBox feed %s: %s', 'clear', 'cache')
        const { context } = mikser.config.whitebox
        let data = {
            context: context || await useMachineId()
        }
        return whiteboxApi('feed', '/api/catalog/clear/cache', data)
    })
    
    onLoaded(async () => {
        const logger = useLogger()
        if (mikser.mikser.options.clear) {
            const { context } = mikser.config.whitebox
            const data = {
                context: context || await useMachineId()
            }
    
            logger.info('WhiteBox feed %s: %s', 'clear', 'catalog')
            await whiteboxApi('feed', '/api/catalog/clear', data)
            clearCache()
        }
    })
    
    onProcessed(async () => {
        const logger = useLogger()
        const { context, services: { feed } } = mikser.config.whitebox || { services: {} }
        if (!feed) return
    
        const entitiesToAdd = useJournal(OPERATION.CREATE, OPERATION.UPDATE)
        .map(operation => operation.entity)
        .filter(_.matches(feed.match || { type: 'document' }))
    
        for (let entity of entitiesToAdd) {
            if (!entity.name || !entity.id) {
                logger.warn(entity, 'WhiteBox feed skipping')
                continue
            }
            logger.trace('WhiteBox feed: %s', entity.id)
            const data = {
                passportId: uuidv1(),
                vaultId: aguid(entity.id),
                refId: entity.name == 'index' ? '/' : '/' + entity.name,
                type: 'mikser.' + entity.meta?.type || entity.type,
                data: _.pick(entity, ['meta', 'stamp', 'content', 'type', 'collection', 'format', 'id', 'uri']),
                date: new Date(entity.time),
                vaults: entity.meta?.vaults,
                context: context || await useMachineId(),
                expire: feed.expire === false ? false : feed.expire || '10 days'
            }
    
            queue.push(async () => {
                clearCache()
                logger.debug('WhiteBox feed %s: %s %s', 'keep', entity.type, data.refId)
                await whiteboxApi('feed', '/api/catalog/keep/one', data)
            })
        }
        entitiesToAdd.length && logger.info('WhiteBox feed %s: %s', 'keep', entitiesToAdd.length)
    
        const entitiesToDelete = useJournal(OPERATION.DELETE)
        .map(operation => operation.entity)
        .filter(_.matches(feed.match || { type: 'document' }))
    
        for (let entity of entitiesToDelete) {
            let data = {
                vaultId: aguid(entity.id),
                context: context || await useMachineId()
            }
    
            if (!mikser.options.clear) {
                queue.push(() => {
                    clearCache()
                    logger.debug('WhiteBox feed %s: %s %s', 'remove', entity.type, entity.id)
                    return whiteboxApi('feed', '/api/catalog/remove', data)
                })
            }
        }
        entitiesToDelete.length && logger.info('WhiteBox feed %s: %s', 'remove', entitiesToDelete.length)
    })
}
