import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(
  executor: IExecutor,
  queue: AsyncIterable<ITask>,
  maxThreads = 0
): Promise<void> {
  const queues = new Map<number, ITask[]>();
  const workers = new Map<number, Promise<void>>();
  const pending = [] as (() => void)[];

  let activeCount = 0;

  const doWork = async (id: number) => {
    const q = queues.get(id)!;
    while (q.length > 0) {
      if (maxThreads > 0 && activeCount >= maxThreads) {
        await new Promise(res => pending.push(res as () => void));
      }

      activeCount++;
      const task = q.shift()!;
      try {
        await executor.executeTask(task);
      } finally {
        activeCount--;
        if (pending.length > 0) pending.shift()!();
      }
    }
    queues.delete(id);
    workers.delete(id);
  };

  for await (const task of queue) {
    const { targetId } = task;
    if (!queues.has(targetId)) queues.set(targetId, []);
    queues.get(targetId)!.push(task);
    if (!workers.has(targetId)) {
      const worker = doWork(targetId);
      workers.set(targetId, worker);
    }
  }

  await Promise.all(workers.values());
}