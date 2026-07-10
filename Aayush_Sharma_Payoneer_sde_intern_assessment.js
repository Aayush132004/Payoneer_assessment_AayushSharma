class CheckpointStore {
  constructor() { this.store = new Map(); }
  async get(jobId) { return this.store.get(jobId) || null; }
  async set(jobId, cp) {
	this.store.set(jobId, JSON.parse(JSON.stringify(cp)));
  }
}
 
class StatsStore {
  constructor() { this.store = new Map(); }
  async save(jobId, stats) { this.store.set(jobId, { ...stats }); }
}
 
// Mock downstream API — fails if id is even
async function processRecord(record) {
  await new Promise(r => setTimeout(r, 10));
  if (parseInt(record.id) % 2 === 0)
	throw new Error(`Validation failed for ${record.id}`);
}
 
class BatchProcessor {
  constructor(checkpointStore, statsStore, processFn, batchSize, maxConcurrency) {
	this.checkpointStore = checkpointStore;
	this.statsStore = statsStore;
	this.processFn = processFn;
	this.batchSize = batchSize;
	this.maxConcurrency = maxConcurrency;
  }
 
  async run(jobId, records) {
	let checkpoint=await this.checkpointStore.get(jobId);
  if(!checkpoint){
    checkpoint={
      last_batch_index:-1,
      stats:{
      completed:0,
      failed:0,
      pending:records.length
      },
      errors:[]
    };
  }
  let stats=checkpoint.stats;
  let errors=checkpoint.errors;

  const totalBatches=Math.ceil(records.length/this.batchSize);

  for(let batchIdx=checkpoint.last_batch_index+1;batchIdx<totalBatches;batchIdx++){
    const start=batchIdx*this.batchSize;
    const end=Math.min(start+this.batchSize,records.length);

    let running=[];
    
    for(let i=start;i<end;i++){
      const task=(async()=>{
        try{
         await this.processFn(records[i]);
         stats.completed++;
        }
        catch(e){
          stats.failed++;
          errors.push({
            index:i,
            error:e.message
          });
        }
        stats.pending--;
      })();
      running.push(task);

      if(running.length===this.maxConcurrency){
        await Promise.all(running);
        running=[];
      }
    }

    if(running.length>0){
      await Promise.all(running);
    }

    await this.statsStore.save(jobId,stats);
    checkpoint.last_batch_index=batchIdx;
    checkpoint.stats=stats;
    checkpoint.errors=errors;
    await this.checkpointStore.set(jobId,checkpoint);
  }

  return {
    success:stats.failed===0,
    partially_processed:stats.failed>0,
    stats,
    errors
  };

  }
}



//test
(async()=>{
  const checkpointStore=new CheckpointStore();
  const statsStore=new StatsStore();
  const processor=new BatchProcessor(checkpointStore,statsStore,processRecord,3,2);

  const records = [
	{id: "1"}, {id: "2"}, {id: "3"},    // batch 0: id "2" fails
	{id: "4"}, {id: "5"}, {id: "6"},    // batch 1: id "4", "6" fail
	{id: "7"},                           // batch 2: all pass
  ];

  const result=await processor.run("job1",records);

  console.log("Test output:",JSON.stringify(result));

})();