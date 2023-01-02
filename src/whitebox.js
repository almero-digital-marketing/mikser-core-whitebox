import { hostname, userInfo } from 'os'
import axios from 'axios'
import { mikser, useLogger } from 'mikser-core'
import MID from 'node-machine-id'

let machineId

export async function api(service, route, data) {
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

export async function useMachineId() {
    if (!machineId) {
        machineId = await MID.machineId() + '_' + hostname() + '_' + userInfo().username
    }
    return machineId
}