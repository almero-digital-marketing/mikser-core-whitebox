import pMap from 'p-map'
import fs from 'fs/promises'
import { createReadStream } from 'node:fs'
import axios from 'axios'
import FormData from 'form-data'
import _ from 'lodash'
import path from 'path'
import { globby } from 'globby'
import { setTimeout } from 'timers/promises'

export default ({ 
    mikser, 
    useLogger, 
    onLoaded, 
    onImport,
    onSync,
    onProcessed, 
    onFinalize,
    useJournal, 
    useMachineId, 
    watch,
    checksum,
    matchEntity,
    findEntity,
    createEntity,
    updateEntity, 
    deleteEntity,
    constants: { ACTION, OPERATION }, 
}) => {
    const collection = 'storage'
    const type = 'file'
    
    const pendingUploads = new Set()
    const history = new Map()
    
    async function upload(fileName, uploadName, uploadChecksum) {
        const logger = useLogger()

        if (pendingUploads.has(fileName)) return
        pendingUploads.add(fileName)
        if (history.get(uploadName) == uploadChecksum) return

        const uploadWhenReady = async () => {
            try {
                const fh = await fs.open(fileName, 0x10000000)
                try {
                    const { context, services: { storage } } = mikser.config.whitebox
                    let data = {
                        file: uploadName,
                        context: context || await useMachineId()
                    }
                    const responseHash = await axios.post(storage.url + '/' + storage.token + '/checksum', data)
                    const matchedHash = responseHash.data.success && uploadChecksum == responseHash.data.hash
                    logger.debug('WhiteBox storage %s: %s %s', 'checksum', fileName, matchedHash)
                    if (!matchedHash) {
                        const uploadHeaders = {
                            expire: storage.expire === false ? false : storage.expire || '10 days',
                            context: data.context
                        }
                        let form = new FormData()
                        form.append(uploadName, createReadStream(fileName))
                        let formHeaders = form.getHeaders()
                        try {
                            const responseUpload = await axios
                            .post(storage.url + '/upload', form, {
                                headers: {
                                    Authorization: 'Bearer ' + storage.token,
                                    ...formHeaders,
                                    ...uploadHeaders,
                                },
                                maxContentLength: Infinity,
                                maxBodyLength: Infinity
                            })
                            if (responseUpload.data.uploads) {
                                for (let file in responseUpload.data.uploads) {
                                    logger.debug('WhiteBox storage %s: %s', 'upload', uploadName)
                                    logger.debug('WhiteBox storage %s: %s', 'link', responseUpload.data.uploads[file])
                                    history.set(uploadName, uploadChecksum)
                                }
                            }							
                        } catch (err) {
                            logger.error('WhiteBox storage upload error: %s', err.message)
                        }
                    } else {
                        history.set(uploadName, uploadChecksum)
                        logger.debug('WhiteBox storage %s: %s', 'skip', uploadName)
                    }
                } catch (err) {
                    logger.error('WhiteBox storage error: %s', err.message)
                }
                fh.close()
            } catch (err) {
                logger.trace(err, 'WhiteBox storage postponed: %s', uploadName)
                await setTimeout(3000)
                await uploadWhenReady()
            }
        }
        await uploadWhenReady()
        pendingUploads.delete(fileName)
    }

    async function link(uploadName) {
        const logger = useLogger()
        const { context, services: { storage } } = mikser.config.whitebox
        let data = {
            file: uploadName,
            context: context || await useMachineId()
        }
        try {
            const response = await axios.post(storage.url + '/' + storage.token + '/link', data)
            logger.debug('WhiteBox storage %s: %s', 'link', response.data?.link)
            return response.data?.link
        } catch (err) {
            logger.trace('WhiteBox storage error: %s', err)
        }
    }
    
    async function unlink(uploadName) {
        const logger = useLogger()
        const { context, services: { storage } } = mikser.config.whitebox
        let data = {
            file: uploadName,
            context: context || await useMachineId()
        }
        try {
            await axios.post(storage.url + '/' + storage.token + '/unlink', data)
            logger.debug('WhiteBox storage: %s %s', 'unlink', uploadName)
        } catch (err) {
            logger.trace('WhiteBox storage error: %s', err)
        }
    }
    
    onImport(async () => {
        const logger = useLogger()
        const paths = await globby('**/*', { cwd: mikser.options.storageFolder })
        logger.info('Importing whitebox storage: %d', paths.length)
    
        return Promise.all(paths.map(async relativePath => {
            const source = path.join(mikser.options.storageFolder, relativePath)
            const uploadName = source.replace(mikser.options.workingFolder, '')

            await createEntity({
                id: path.join(`/${collection}`, relativePath),
                uri: uploadName,
                collection,
                type,
                format: path.extname(relativePath).substring(1).toLowerCase(),
                name: relativePath,
                source,
                checksum: await checksum(source),
                link: await link(uploadName)
            })
        }))
    })

    onProcessed(async (signal) => {
        const logger = useLogger()
        const { services: { storage } } = mikser.config.whitebox || { services: {} }
        if (!storage) return
    
        let added = 0
        let deleted = 0
        await pMap(useJournal('WhiteBox storage processing', [OPERATION.CREATE, OPERATION.UPDATE, OPERATION.DELETE], signal), async ({ entity, operation }) => {
            const match = storage.match || ((entity) => entity.id.indexOf('/storage/') != -1)
            if (matchEntity(entity, match)) {
                const uploadName = entity.source.replace(mikser.options.workingFolder, '')
                switch (operation) {
                    case OPERATION.CREATE:
                    case OPERATION.UPDATE:
                        added++
                        await upload(entity.source, uploadName, entity.checksum)
                    break
                    case OPERATION.DELETE:
                        deleted++
                        await unlink(uploadName)
                    break
                }
            }
        }, { concurrency: 4, signal })

        logger.debug('WhiteBox storage %s: %s', 'upload', added)
        logger.debug('WhiteBox storage %s: %s', 'unlink', deleted)
    })

    onFinalize(async (signal) => {
        const logger = useLogger()
        const { services: { storage } } = mikser.config.whitebox || { services: {} }
        if (!storage) return

        await pMap(useJournal('WhiteBox storage output', [OPERATION.RENDER], signal), async ({ entity, output }) => {
            if (output?.success) {                
                if (storage.match && storage.match(entity) || !storage.match && entity.id.indexOf('/storage/') != -1 ) {
                    const uploadName = entity.destination.replace(mikser.options.outputFolder, '').replace(mikser.options.workingFolder, '')
                    try {
                        const uploadChecksum = await checksum(entity.destination)
                        await upload(entity.destination, uploadName, uploadChecksum)
                    } catch (err) {
                        if (err.code == 'ENOENT') {
                            logger.error('Output is missing: %s', entity.destination)
                        } else {
                            logger.error('WhiteBox storage error: %s', err.message)
                        }
                    }
                }
            }
        }, { concurrency: 4, signal })
    })

    onLoaded(async () => {
        const logger = useLogger()
        const { context, services: { storage } } = mikser.config.whitebox
        if (!storage) return

        mikser.options.storage = storage?.storageFolder || collection
        mikser.options.storageFolder = path.join(mikser.options.workingFolder, mikser.options.storage)

        if (mikser.options.clear) {
            const data = {
                context: context || await useMachineId()
            }
            try {
                logger.info('WhiteBox storage: %s', 'clear')
                await axios.post(storage.url + '/' + storage.token + '/clear', data)
            } catch (err) {
                logger.error('WhiteBox storage error: %s', err.message)
            }
        }

        logger.info('WhiteBox storage folder: %s', mikser.options.storageFolder)
        await fs.mkdir(mikser.options.storageFolder, { recursive: true })
    
        watch(collection, mikser.options.storageFolder)
    })

    onSync(collection, async ({ action, context }) => {
        if (!context.relativePath) return false
        const { relativePath } = context
    
        const source = path.join(mikser.options.filesFolder, relativePath)
        const format = path.extname(relativePath).substring(1).toLowerCase()
        const id = path.join(`/${collection}`, relativePath)
        const uploadName = source.replace(mikser.options.workingFolder, '')
        
        let synced = true
        switch (action) {
            case ACTION.CREATE:
                await createEntity({
                    id,
                    uri: uploadName,
                    name: relativePath,
                    collection,
                    type,
                    format,
                    source,
                    checksum: await checksum(source),
                    link: await link(uploadName)
                })
            break
            case ACTION.UPDATE:
                const current = await findEntity({ id })
                if (current?.checksum != checksum) {
                    await updateEntity({
                        id,
                        uri: uploadName,
                        name: relativePath,
                        collection,
                        type,
                        format,
                        source,
                        checksum: await checksum(source),
                        link: await link(uploadName)
                    })
                } else {
                    synced = false
                }
            break
            case ACTION.DELETE:
                await unlink(uploadName)
                await deleteEntity({
                    id,
                    collection,
                    type,
                })
            break
        }
        return synced
    })
    
    return {
        collection,
        type
    }
}