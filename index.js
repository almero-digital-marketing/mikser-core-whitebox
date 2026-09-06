import { hostname, userInfo } from 'os'
import axios from 'axios'
import MID from 'node-machine-id'

import feed from './src/feed.js'
import storage from './src/storage.js'

export function whitebox(options = {}) {
    return (core) => {
    const { runtime, useLogger, onLoaded } = core

    // Lazy migration: feed.js / storage.js still read runtime.config.whitebox
    // directly. Mirror options into that slot so internal helpers keep
    // working without a per-file refactor. Plugin-side reads inside this
    // factory use `options.X` and the mirror gets removed when the
    // helpers move off the config slot.
    runtime.config.whitebox = options

    async function whiteboxApi(service, route, data) {
        const logger = useLogger()
        const { services } = runtime.config.whitebox
        const { url, token } = services[service]
        if (!url || !token) return

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
        logger.info('WhiteBox context: %s', options.context)
    })

    return {
        whiteboxApi,
        useMachineId,
        ...feed({ ...core, whiteboxApi, useMachineId }),
        ...storage({ ...core, whiteboxApi, useMachineId }), module: import.meta.url,
    }
    }
}