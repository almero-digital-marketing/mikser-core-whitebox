import { mikser, onImport, useLogger, constants, onLoaded, onSync, watchEntities, createEntity, updateEntity, deleteEntity, onProcessed, useOperations } from 'mikser-core'
import { useMachineId } from './whitebox.js'
import Queue from 'queue'
import fs from 'fs/promises'
import { createReadStream } from 'fs'
import axios from 'axios'
import hasha from 'hasha'
import { globby } from 'globby'
import { join, extname } from 'path'
import FormData from 'form-data'

let queue = Queue({
    concurrency: 4,
    autostart: true
})

const pendingUploads = {}

async function upload(entity) {
    const logger = useLogger()
    const relativePath = entity.source.replace(mikser.options.storageFolder, '')
    const file = join(mikser.options.storageFolder, relativePath)
    if (pendingUploads[file]) return
    pendingUploads[file] = true
    try {
        const fh = await fs.open(file, fs.constants.O_RDONLY | 0x10000000)

        try {
            const { global, services: { storage } } = mikser.config.whitebox
            let data = {
                file: relativePath
            }
            if (!global) data.context = await useMachineId()
            const responseHash = await axios.post(storage.url + '/' + storage.token + '/hash', data)
            const matchedHash = responseHash.data.success && entity.checksum == responseHash.data.hash
            logger.debug('WhiteBox storage: %s %s %s', 'hash', file, matchedHash)
            if (!matchedHash) {
                let uploadHeaders = {}
                if (!global) {
                    uploadHeaders = {
                        expire: storage.expire,
                        context: data.context
                    }
                }
                let form = new FormData()
                form.append(relativePath, createReadStream(file))
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
                            logger.debug('WhiteBox storage: %s %s', 'upload', file)
                            logger.info('WhiteBox storage: %s %s', 'link', responseUpload.data.uploads[file])
                        }
                    }							
                } catch (err) {
                    logger.error(err, 'WhiteBox storage upload error')
                }
            }
        } catch (err) {
            logger.error(err, 'WhiteBox storage error')
        }

        fh.close()
    } catch (err) {
        logger.trace(err, 'WhiteBox storage skipped: %s', relativePath)
    }

    delete pendingUploads[file]
}

async function unlink(relativePath) {
    const logger = useLogger()
    const { global, services: { storage } } = mikser.config.whitebox
    let data = {
        file: relativePath
    }
    if (!global) data.context = await useMachineId()
    try {
        await axios.post(storage.url + '/' + storage.token + '/unlink', data)
        logger.debug('WhiteBox storage: %s %s', 'unlink', relativePath)
    } catch (err) {
        logger.trace('WhiteBox storage error: %s', err)
    }
}

async function link(file) {
    const logger = useLogger()
    const { global, services: { storage } } = mikser.config.whitebox
    let data = {
        file
    }
    if (!global) data.context = await useMachineId()
    const response = await axios.post(storage.url + '/' + storage.token + '/link', data)
    return response.data.link
}

onLoaded(async () => {
    const logger = useLogger()
    const { global, services: { storage } } = mikser.config.whitebox
    if (!storage) return

    mikser.options.storageFolder = storage?.storageFolder || join(mikser.options.workingFolder, 'storage')

    logger.info('Storage: %s', mikser.options.storageFolder)
    await fs.mkdir(mikser.options.storageFolder, { recursive: true })

    watchEntities('storage', mikser.options.storageFolder)

    if (mikser.options.clear) {
        const data = {}
        if (global) {
            data.context = await useMachineId()
        }
        try {
            logger.info('WhiteBox storage: %s', 'clear')
            await axios.post(storage.url + '/' + storage.token + '/clear', {})
        } catch (err) {
            logger.error('WhiteBox storage error: %s', err)
        }
    }
})

onSync(async ({ id, operation }) => {
    const relativePath = id.replace('/storage/', '')

    const uri = await link(id)
    const source = join(mikser.options.storageFolder, relativePath)
    const format = extname(relativePath).substring(1).toLowerCase()
    
    switch (operation) {
        case constants.OPERATION_CREATE:
            var checksum = await hasha.fromFile(source, { algorithm: 'md5' })
            await createEntity({
                id,
                uri,
                name: relativePath.replace(extname(relativePath), ''),
                collection: 'files',
                type: 'storage',
                format,
                source,
                checksum
            })
        break
        case constants.OPERATION_UPDATE:
            var checksum = await hasha.fromFile(source, { algorithm: 'md5' })
            await updateEntity({
                id,
                uri,
                name: relativePath.replace(extname(relativePath), ''),
                collection: 'files',
                type: 'storage',
                format,
                source,
                checksum
            })
        break
        case constants.OPERATION_DELETE:
            await deleteEntity({
                id,
                collection: 'files',
                type: 'storage',
                checksum
            })
        break
    }
})

onProcessed(async () => {
    const logger = useLogger()

    const filesToUpload = useOperations([constants.OPERATION_CREATE, constants.OPERATION_UPDATE])
    .map(operation => operation.entity)
    .filter(entity => !entity.layout && entity.type == 'storage')

    for (let entity of filesToUpload) {
        queue.push(async () => await upload(entity))
    }
    filesToUpload.length && logger.info('WhiteBox storage: %s %s', 'upload', filesToUpload.length)

    const entitiesToUnlink = useOperations([constants.OPERATION_DELETE])
    .map(operation => operation.entity)
    .filter(entity => !entity.layout && entity.type == 'storage')

    for (let entity of entitiesToUnlink) {
        const relativePath = entity.id.replace('/storage', '')
        queue.push(async () => await unlink(relativePath))
    }
    entitiesToUnlink.length && logger.info('WhiteBox storage: %s %s', 'unlink', entitiesToUnlink.length)
})

onImport(async () => {
    await fs.mkdir(mikser.options.storageFolder, { recursive: true }) 
    const paths = await globby('**/*', { cwd: mikser.options.storageFolder })
    return Promise.all(paths.map(async relativePath => {
        const id = join('/storage', relativePath)
        const uri = await link(id)
        const source = join(mikser.options.storageFolder, relativePath)
        const checksum = await hasha.fromFile(source, { algorithm: 'md5' })

        await createEntity({
            id,
            uri,
            collection: 'files',
            type: 'storage',
            format: extname(relativePath).substring(1).toLowerCase(),
            name: relativePath,
            source,
            checksum
        })
    }))
})