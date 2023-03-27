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

    let types = new Set()
    queue.on('end', async () => {
        const logger = useLogger()
        const { context } = mikser.config.whitebox
        for(let type of types) {
            await whiteboxApi('feed', '/api/catalog/expire', {
                context: context || await useMachineId(),
                stamp: mikser.stamp,
                type
            })
        }
        clearCache()
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
        if (mikser.options.clear) {
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
    
        let added = 0
        let deleted = 0
        for await (let { entity, operation } of useJournal('WhiteBox feed', [OPERATION.CREATE, OPERATION.UPDATE, OPERATION.DELETE])) {
            if (entity.meta && (feed.match && feed.match(entity) || !feed.match && entity.type == 'document')) {
                switch (operation) {
                    case OPERATION.CREATE:
                    case OPERATION.UPDATE:
                        added++
                        if (!entity.name || !entity.id) {
                            logger.warn(entity, 'WhiteBox feed skipping')
                            continue
                        }
                        logger.trace('WhiteBox feed: %s', entity.id)
                        const keepData = {
                            passportId: uuidv1(),
                            vaultId: aguid(entity.id),
                            refId: '/' + entity.name.replace('index', ''),
                            type: 'mikser.' + (entity.meta.type || entity.type),
                            data: _.pick(entity, ['meta', 'stamp', 'content', 'type', 'collection', 'format', 'id', 'uri']),
                            date: new Date(entity.time),
                            vaults: entity.meta.vaults,
                            context: context || await useMachineId(),
                            expire: feed.expire === false ? false : feed.expire || '10 days'
                        }
                        types.add(keepData.type)
                
                        queue.push(async () => {
                            logger.debug('WhiteBox feed %s: %s %s', 'keep', entity.type, keepData.refId)
                            await whiteboxApi('feed', '/api/catalog/keep/one', keepData)
                        })
                    break
                    case OPERATION.DELETE:
                        deleted++
                        const removeData = {
                            vaultId: aguid(entity.id),
                            context: context || await useMachineId()
                        }
                
                        if (!mikser.options.clear) {
                            queue.push(() => {
                                logger.debug('WhiteBox feed %s: %s %s', 'remove', entity.type, entity.id)
                                return whiteboxApi('feed', '/api/catalog/remove', removeData)
                            })
                        }
                    break
                }
            }
        }
        added && logger.info('WhiteBox feed %s: %s', 'keep', added)
        deleted && logger.info('WhiteBox feed %s: %s', 'remove', deleted)
    })
}
