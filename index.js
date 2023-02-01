import { hostname, userInfo } from 'os'
import axios from 'axios'
import MID from 'node-machine-id'

import feed from './src/feed.js'
import storage from './src/storage.js'

export default (core) => {
    const { mikser, useLogger, onLoaded } = core
    
    async function whiteboxApi(service, route, data) {
        const logger = useLogger()
        const { services } = mikser.config.whitebox
        const { url, token } = services[service]
        if (!url || !token ) return
    
        try {
            const response = await axios.post(url + route + '?stamp=' + Date.now(), data, {
                headers: {
                    Authorization: 'Bearer ' + token,
                }
            })
            if (response.data.success) return response.data
        } catch (err) {
            logger.error(err, 'WhiteBox system error: %s %o', route, data)
        }
    }
    
    let machineId
    async function useMachineId() {
        if (!machineId) {
            machineId = await MID.machineId() + '_' + hostname() + '_' + userInfo().username
        }
        return machineId
    }

    onLoaded(() => {
        const logger = useLogger()
        logger.info('WhiteBox context: %s', mikser.config.whitebox?.context)
    })

    return {
        whiteboxApi, 
        useMachineId,
        ...feed({ ...core, whiteboxApi, useMachineId }),
        ...storage({ ...core, whiteboxApi, useMachineId }),
    }

}