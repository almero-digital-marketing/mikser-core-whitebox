import pMap from 'p-map'
import { v1 as uuidv1 } from 'uuid'
import aguid from 'aguid'
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
    let types = new Set()

    async function expireCatalog() {
        const { context } = mikser.config.whitebox
        for(let type of types) {
            await whiteboxApi('feed', '/api/catalog/expire', {
                context: context || await useMachineId(),
                stamp: mikser.stamp,
                type
            })
        }
    }
    
    async function clearCache() {
        const logger = useLogger()
        logger.debug('WhiteBox feed %s: %s', 'clear', 'cache')
        const { context } = mikser.config.whitebox
        let data = {
            context: context || await useMachineId()
        }
        return whiteboxApi('feed', '/api/catalog/clear/cache', data)
    }
    
    onLoaded(async () => {
        const logger = useLogger()
        if (mikser.options.clear) {
            const { context } = mikser.config.whitebox
            const data = {
                context: context || await useMachineId()
            }
    
            logger.debug('WhiteBox feed %s: %s', 'clear', 'catalog')
            await whiteboxApi('feed', '/api/catalog/clear', data)
            await clearCache()
        }
    })
    
    onProcessed(async (signal) => {
        const logger = useLogger()
        const { context, services: { feed } } = mikser.config.whitebox || { services: {} }
        if (!feed) return
    
        let added = 0
        let deleted = 0
        await pMap(useJournal('WhiteBox feed', [OPERATION.CREATE, OPERATION.UPDATE, OPERATION.DELETE], signal), async ({ entity, operation }) => {
            if (entity.meta && (feed.match && feed.match(entity) || !feed.match && entity.type == 'document')) {
                switch (operation) {
                    case OPERATION.CREATE:
                    case OPERATION.UPDATE:
                        added++
                        if (!entity.name || !entity.id) {
                            logger.warn(entity, 'WhiteBox feed skipping')
                            return
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
                
                        logger.debug('WhiteBox feed %s: %s %s', 'keep', entity.type, keepData.refId)
                        await whiteboxApi('feed', '/api/catalog/keep/one', keepData)
                    break
                    case OPERATION.DELETE:
                        deleted++
                        const removeData = {
                            vaultId: aguid(entity.id),
                            context: context || await useMachineId()
                        }
                
                        if (!mikser.options.clear) {
                            logger.debug('WhiteBox feed %s: %s %s', 'remove', entity.type, entity.id)
                            return whiteboxApi('feed', '/api/catalog/remove', removeData)
                        }
                    break
                }
            }
        }, { concurrency: 4, signal })
        logger.debug('WhiteBox feed %s: %s', 'keep', added)
        logger.debug('WhiteBox feed %s: %s', 'remove', deleted)

        await expireCatalog()
        await clearCache()
    })
}
