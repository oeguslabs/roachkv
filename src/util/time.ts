export function sleep(num: number): Promise<void> {
    return new Promise(resolve => {
        setTimeout(resolve, num)
    })
}
