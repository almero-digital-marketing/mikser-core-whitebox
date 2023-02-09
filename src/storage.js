import Queue from 'queue'
import fs from 'fs/promises'
import { createReadStream } from 'node:fs'
import axios from 'axios'
import hasha from 'hasha'
import { globby } from 'globby'
import { join, extname } from 'node:path'
import FormData from 'form-data'

export default ({ 
    mikser, 
    onImport, 
    useLogger, 
    onLoaded, 
    onSync, 
    watch, 
    createEntity, 
    updateEntity, 
    deleteEntity, 
    onProcessed, 
    useJournal, 
    useMachineId, 
    constants: { ACTION, OPERATION }, 
}) => {

    const collection = 'files'
    const type = 'storage'
    
    let queue = Queue({
        concurrency: 4,
        autostart: true
    })
    queue.on('end', () => {
        const logger = useLogger()
        logger.info('WhiteBox storage completed')
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
                const { context, services: { storage } } = mikser.config.whitebox
                let data = {
                    file: relativePath,
                    context: context || await useMachineId()
                }
                const responseHash = await axios.post(storage.url + '/' + storage.token + '/hash', data)
                const matchedHash = responseHash.data.success && entity.checksum == responseHash.data.hash
                logger.debug('WhiteBox storage %s: %s %s', 'hash', file, matchedHash)
                if (!matchedHash) {
                    const uploadHeaders = {
                        expire: storage.expire === false ? false : storage.expire || '10 days',
                        context: data.context
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
                                logger.debug('WhiteBox storage %s: %s', 'upload', file)
                                logger.info('WhiteBox storage %s: %s', 'link', responseUpload.data.uploads[file])
                            }
                        }							
                    } catch (err) {
                        logger.error('WhiteBox storage upload error: %s', err.message)
                    }
                }
            } catch (err) {
                logger.error('WhiteBox storage error: %s', err.message)
            }
    
            fh.close()
        } catch (err) {
            logger.trace(err, 'WhiteBox storage skipped: %s', relativePath)
        }
    
        delete pendingUploads[file]
    }
    
    async function unlink(relativePath) {
        const logger = useLogger()
        const { context, services: { storage } } = mikser.config.whitebox
        let data = {
            file: relativePath,
            context: context || await useMachineId()
        }
        try {
            await axios.post(storage.url + '/' + storage.token + '/unlink', data)
            logger.debug('WhiteBox storage: %s %s', 'unlink', relativePath)
        } catch (err) {
            logger.trace('WhiteBox storage error: %s', err)
        }
    }
    
    async function link(file) {
        const { context, services: { storage } } = mikser.config.whitebox
        let data = {
            file,
            context: context || await useMachineId()
        }
        const response = await axios.post(storage.url + '/' + storage.token + '/link', data)
        return response.data.link
    }
    
    onLoaded(async () => {
        const logger = useLogger()
        const { context, services: { storage } } = mikser.config.whitebox
        if (!storage) return
    
        mikser.options.storageFolder = storage?.storageFolder || join(mikser.options.workingFolder, type)
    
        logger.info('Storage: %s', mikser.options.storageFolder)
        await fs.mkdir(mikser.options.storageFolder, { recursive: true })
    
        watch('storage', mikser.options.storageFolder)
    
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
    })
    
    onSync(type, async ({ action, context: { relativePath } }) => {
        if (!relativePath) return false
    
        const id = path.join(`/${collection}`, relativePath)
        const uri = await link(id)
        const source = join(mikser.options.storageFolder, relativePath)
        const format = extname(relativePath).substring(1).toLowerCase()
        
        switch (action) {
            case ACTION.CREATE:
                var checksum = await hasha.fromFile(source, { algorithm: 'md5' })
                await createEntity({
                    id,
                    uri,
                    name: relativePath.replace(extname(relativePath), ''),
                    collection,
                    type,
                    format,
                    source,
                    checksum
                })
            break
            case ACTION.UPDATE:
                var checksum = await hasha.fromFile(source, { algorithm: 'md5' })
                await updateEntity({
                    id,
                    uri,
                    name: relativePath.replace(extname(relativePath), ''),
                    collection,
                    type,
                    format,
                    source,
                    checksum
                })
            break
            case ACTION.DELETE:
                await deleteEntity({
                    id,
                    collection,
                    type,
                    checksum
                })
            break
        }
    })
    
    onProcessed(async () => {
        const logger = useLogger()
    
        let uploaded = 0
        for (let { entity } of useJournal(OPERATION.CREATE, OPERATION.UPDATE)) {
            if (!entity.layout && entity.type == type) {
                uploaded++
                queue.push(async () => await upload(entity))
            }
        }
        uploaded && logger.info('WhiteBox storage: %s %s', 'upload', uploaded)
        
        const unlinked = 0
        for (let { entity } of useJournal(OPERATION.DELETE)) {
            if (!entity.layout && entity.type == type) {
                unlinked++
                const relativePath = entity.id.replace(`/${type}`, '')
                queue.push(async () => await unlink(relativePath))
            }
        }
        unlinked && logger.info('WhiteBox storage: %s %s', 'unlink', unlinked)
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
                collection,
                type,
                format: extname(relativePath).substring(1).toLowerCase(),
                name: relativePath,
                source,
                checksum
            })
        }))
    })

    return {
        collection,
        type
    }
}