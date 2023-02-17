import Queue from 'queue'
import fs from 'fs/promises'
import { createReadStream } from 'node:fs'
import axios from 'axios'
import hasha from 'hasha'
import FormData from 'form-data'
import _ from 'lodash'

export default ({ 
    mikser, 
    useLogger, 
    onLoaded, 
    onProcessed, 
    onFinalize,
    useJournal, 
    useMachineId, 
    constants: { OPERATION }, 
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
    
    async function upload(entity, property) {
        const logger = useLogger()
        const fileName = _.get(entity, property)
        let uploadName = fileName.replace(mikser.options.workingFolder, '').split('/storage/').pop()
        uploadName = (uploadName[0] == '/' ? '/storage' : '/storage/') + uploadName

        if (pendingUploads[fileName]) return
        pendingUploads[fileName] = true
        try {
            const fh = await fs.open(fileName, fs.constants.O_RDONLY | 0x10000000)
            try {
                const { context, services: { storage } } = mikser.config.whitebox
                let data = {
                    file: uploadName,
                    context: context || await useMachineId()
                }
                const responseHash = await axios.post(storage.url + '/' + storage.token + '/hash', data)
                const fileHash = await hasha.fromFile(fileName, { algorithm: 'md5' })
                const matchedHash = responseHash.data.success && fileHash == responseHash.data.hash
                logger.debug('WhiteBox storage %s: %s %s', 'hash', fileName, matchedHash)
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
                                logger.info('WhiteBox storage %s: %s', 'link', responseUpload.data.uploads[file])
                            }
                        }							
                    } catch (err) {
                        logger.error('WhiteBox storage upload error: %s', err.message)
                    }
                } else {
                    const response = await axios.post(storage.url + '/' + storage.token + '/link', data)
                    logger.debug('WhiteBox storage %s: %s', 'skip', uploadName)
                    logger.info('WhiteBox storage %s: %s', 'link', response.data.link)
                }
            } catch (err) {
                logger.error('WhiteBox storage error: %s', err.message)
            }
    
            fh.close()
        } catch (err) {
            logger.trace(err, 'WhiteBox storage skipped: %s', uploadName)
        }
    
        delete pendingUploads[fileName]
    }
    
    async function unlink(entity, property) {
        const logger = useLogger()
        const fileName = _.get(entity, property)
        const uploadName = '/storage' + fileName.split('/storage/').pop()
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
        
    onProcessed(async () => {
        const logger = useLogger()
        const { services: { storage } } = mikser.config.whitebox || { services: {} }
        if (!storage) return
    
        let added = 0
        let deleted = 0
        for (let { entity, operation } of useJournal(OPERATION.CREATE, OPERATION.UPDATE, OPERATION.DELETE)) {
            if (storage.match && storage.match(entity) || !storage.match && entity.id.indexOf('/storage/') != -1 ) {
                switch (operation) {
                    case OPERATION.CREATE:
                    case OPERATION.UPDATE:
                        added++
                        queue.push(() => upload(entity, 'source'))
                    break
                    case OPERATION.DELETE:
                        deleted++
                        queue.push(() => unlink(entity, 'source'))
                    break
                }
            }
        }
        added && logger.info('WhiteBox storage %s: %s', 'upload', added)
        deleted && logger.info('WhiteBox storage %s: %s', 'unlink', deleted)
    })

    onFinalize(async () => {
        const { services: { storage } } = mikser.config.whitebox || { services: {} }
        if (!storage) return

        for(let { entity } of useJournal(OPERATION.RENDER)) {
            if (storage.match && storage.match(entity) || !storage.match && entity.id.indexOf('/storage/') != -1 ) {
                queue.push(() => upload(entity, 'destination'))
            }
        }
    })

    onLoaded(async () => {
        const logger = useLogger()
        const { context, services: { storage } } = mikser.config.whitebox
        if (!storage) return
       
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
    
    return {
        collection,
        type
    }
}