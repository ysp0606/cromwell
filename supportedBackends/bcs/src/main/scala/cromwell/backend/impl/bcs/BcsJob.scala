package cromwell.backend.impl.bcs

import com.aliyuncs.batchcompute.pojo.v20151111._
import com.aliyun.batchcompute2.Client
import com.aliyun.batchcompute2.models._
import com.aliyun.batchcompute2.models.VPC
import com.aliyun.batchcompute2.models.{CreateJobRequest, CreateJobResponse}
import com.aliyun.batchcompute2.models.{GetJobRequest, GetJobResponse}
import cromwell.core.ExecutionEvent
import cromwell.core.path.Path

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object BcsJob{
  val BcsDockerImageEnvKey = "BATCH_COMPUTE_DOCKER_IMAGE"
  val BcsDockerPathEnvKey = "BATCH_COMPUTE_DOCKER_REGISTRY_OSS_PATH"
  val BcsDefaultResourceType = "OnDemand"
  val BcsDefaultInstanceType = "ecs.c6.large"
  val BcsDefaultImageId = "img-ubuntu-vpc"
  val BcsJobType = "Batch"

  val BcsResourceKeyCpu = "cpu"
  val BcsResourceKeyMemory = "memory"
}

final case class BcsJob(name: String,
                  description: String,
                  commandString: String,
                  packagePath: Path,
                  mounts: Seq[BcsMount],
                  envs: Map[String, String],
                  runtime: BcsRuntimeAttributes,
                  stdoutPath: Option[Path],
                  stderrPath: Option[Path],
                  batchCompute: Client) {

  lazy val lazyDisks = new Disks
  lazy val lazyConfigs = new Configs
  lazy val lazyVpc = new VPC
  lazy val lazyTask = new TaskDescription
//  lazy val lazyJob = new JobDescription
  lazy val lazyJobDefinition = new JobDefinition
  lazy val lazyCmd = new Command
  lazy val lazyEcs = new ECS
  lazy val lazyRuntime = new Runtimes
  lazy val lazyResource: Map[String, String] = Map()

  def submit(): Try[String] = Try{
    val request: CreateJobRequest = new CreateJobRequest
    request.setProject(project)
    request.setName(name)
    request.setDescription(description)
    request.setDefinition(jobDefinition)
    val response: CreateJobResponse = batchCompute.CreateJob(request)
    val jobId = response.getJobId
    jobId
  }

  def getStatus(jobId: String): Try[RunStatus] = Try{
    val request: GetJobRequest = new GetJobRequest
    request.setProject(project)
    request.setJobId(jobId)
    val response: GetJobResponse = batchCompute.GetJob(request)
//    val job = response.getJobId
    val status = response.getStatus.state
    //Todo: message
    val message = ""
    val eventList = Seq[ExecutionEvent]()
    RunStatusFactory.getStatus(jobId, status, Some(message), Some(eventList)) match {
      case Success(status) => status
      case Failure(e) => throw e
    }
  }

  def cancel(jobId: String): Unit = {
    // XXX: Do nothing currently.
  }

  private[bcs] def systemDisk: Option[SystemDisk] = runtime.systemDisk map { disk =>
      val systemDisk = new SystemDisk()
      systemDisk.setType(disk.diskType)
      systemDisk.setSize(disk.sizeInGB)
      systemDisk
  }

  private[bcs] def dataDisk: Option[DataDisk] = runtime.dataDisk map { disk =>
    val dataDisk = new DataDisk
    dataDisk.setType(disk.diskType)
    dataDisk.setSize(disk.sizeInGB)
    dataDisk.setMountPoint(disk.mountPoint)
    dataDisk
  }

  val ecs: Option[ECS] = {
    // Set default disk
    lazyEcs.setSystemDiskType(BcsSystemDisk.Default.diskType)
    lazyEcs.setSystemDiskSize( BcsSystemDisk.Default.sizeInGB)

    runtime.disks map { disk =>
      def setDisk(d: BcsDisk) = {
        d match {
          case _: BcsSystemDisk =>
            lazyEcs.setSystemDiskType(d.diskType)
            lazyEcs.setSystemDiskSize(d.sizeInGB)
          case _: BcsDataDisk =>
            //Todo: data disk support
            lazyEcs.setSystemDiskType(d.diskType)
            lazyEcs.setSystemDiskSize(d.sizeInGB)
        }
      }

      disk map {d => setDisk(d)}
    }
    Option(lazyEcs)
  }

//  // XXX: maybe more elegant way to reduce two options?
//  private[bcs] def disks: Option[Disks] = {
//    (systemDisk, dataDisk) match {
//      case (Some(sys), Some(data)) =>
//        lazyDisks.setSystemDisk(sys)
//        lazyDisks.setDataDisk(data)
//        Some(lazyDisks)
//      case (Some(sys), None) =>
//        standardDisks
//      case (None, Some(data)) =>
//        lazyDisks.setDataDisk(data)
//        Some(lazyDisks)
//      case (None, None) =>
//        lazyDisks.setSystemDisk(defaultSystemDisk)
//        standardDisks
//        Some(lazyDisks)
//    }
//  }

  val project: String = {
    runtime.project.getOrElse("default")
  }

  val vpc: Option[VPC] = {
    (runtime.vpc flatMap  {v => v.vpcId}, runtime.vpc flatMap {v => v.vSwitchId}) match {
      case (Some(id), Some(switchId)) =>
        lazyVpc.setVSwitches(switchId.asJava)
        lazyVpc.setVPCId(id)
        Some(lazyVpc)
      case (_, _) => None
    }
    }

//  private[bcs] def configs: Option[Configs] = {
//    (vpc, disks) match {
//      case (Some(bcsVpc), Some(bcsDisks)) =>
//        lazyConfigs.setDisks(bcsDisks)
//        val networks = new Networks
//        networks.setVpc(bcsVpc)
//        lazyConfigs.setNetworks(networks)
//        Some(lazyConfigs)
//      case (Some(bcsVpc), None) =>
//        val networks = new Networks
//        networks.setVpc(bcsVpc)
//        lazyConfigs.setNetworks(networks)
//        Some(lazyConfigs)
//      case (None, Some(bcsDisks)) =>
//        lazyConfigs.setDisks(bcsDisks)
//        Some(lazyConfigs)
//      case (None, None) => None
//    }
//  }
//
//  private[bcs] def params: Parameters = {
//    val parames = new Parameters
//    lazyCmd.setPackagePath(packagePath.pathAsString)
//    lazyCmd.setEnvVars(environments.asJava)
//    lazyCmd.setCommandLine(commandString)
//
////    dockers foreach {docker => lazyCmd.setDocker(docker)}
//    stdoutPath foreach {path => parames.setStdoutRedirectPath(path.normalize().pathAsString + "/")}
//    stderrPath foreach {path => parames.setStderrRedirectPath(path.normalize().pathAsString + "/")}
//
//    parames.setCommand(lazyCmd)
//    parames
//  }

  private[bcs] def runtimes: Runtimes = {
    ecs foreach { e => lazyRuntime.setECS(e)}
    dockers foreach {docker => lazyRuntime.setDocker(docker)}
    vpc foreach {v => lazyRuntime.setVPC(v) }
    val cls = runtime.cluster.getOrElse(throw new Exception("Only cls-xxxx is supported"))
    cls.fold(handleManagedJobQueue, handleServerlessJob)
    lazyRuntime
  }

  private[bcs] def environments: Map[String, String] = {
    runtime.docker match {
      case None =>
        runtime.dockerTag match {
          case Some(docker: BcsDockerWithoutPath) => envs + (BcsJob.BcsDockerImageEnvKey -> docker.image)
          case Some(docker: BcsDockerWithPath) => envs + (BcsJob.BcsDockerPathEnvKey -> docker.path) + (BcsJob.BcsDockerImageEnvKey -> docker.image)
          case _ => envs
        }
      case _ => envs
    }
  }

  val dockers: Option[Docker] = {
    runtime.docker match {
      case Some(docker: BcsDockerWithoutPath) =>
        val dockers = new Docker
        dockers.setImage(docker.image)
        Some(dockers)
      case _ => None
    }
  }

//  private[bcs] def jobDesc: JobDescription = {
//    lazyJob.setName(name)
//    lazyJob.setDescription(description)
//    lazyJob.setType("DAG")
//
//    val dag = new DAG
//    dag.addTask("cromwell", taskDesc)
//    lazyJob.setDag(dag)
//
//    // NOTE: Do NOT set auto release here or we will not be able to get status after the job completes.
//    lazyJob.setAutoRelease(false)
//
//    lazyJob
//  }

  private[bcs] def failStrategy = {
    val fs = new FailStrategy
    //Todo: add new runtime: retries and waiting timeout
    fs.setMaxRetries(3)
    runtime.timeout foreach {t => fs.setRunningTimeout(t)}
    fs.setWaitingTimeout(999)
    fs
  }

  private[bcs] def jobDefinition: JobDefinition = {
    lazyJobDefinition.setRuntimes(runtimes)
    lazyJobDefinition.setCommand(List(commandString).asJava)
    lazyJobDefinition.setEnvs(envs.asJava)
    lazyJobDefinition.setType(BcsJob.BcsJobType)
    lazyJobDefinition.setResources(jobResource.asJava)
    lazyJobDefinition.setFailStrategy(failStrategy)
//    lazyJobDefinition.setLoggings()

//    val mnts = new Mounts
//    val volumes = List[Volume]()
//    mounts foreach  {
//      case input: BcsInputMount =>
//        volumes++ input.toBcsVolume
//      case output: BcsOutputMount =>
//        var srcStr = BcsMount.toString(output.src)
//        if (BcsMount.toString(output.dest).endsWith("/") && !srcStr.endsWith("/")) {
//          srcStr += "/"
//        }
//        lazyTask.addOutputMapping(srcStr, BcsMount.toString(output.dest))
//    }

    val inputs = mounts.filter(_.isInstanceOf[BcsInputMount])
    val outputs = mounts.filter(_.isInstanceOf[BcsOutputMount])

    val volumes = inputs.map(i => i.toBcsVolume)++outputs.map(o => o.toBcsVolume)
    val mountPoint = inputs.map(i => i.toBcsMountPoint)++outputs.map(o => o.toBcsMountPoint)

    lazyJobDefinition.setVolumes(volumes.asJava)
    lazyJobDefinition.setMountPoints(mountPoint.asJava)

    lazyJobDefinition
  }

//  // Todo: for bc 2.0, cpu/memory is needed
//  private def getServerlessClusterConfiguration(runtime: BcsRuntimeAttributes): BcsClusterIdOrConfiguration = {
//    val instanceType = runtime.instanceType.getOrElse(BcsJob.BcsDefaultInstanceType)
//    val imageId = runtime.imageId.getOrElse(BcsJob.BcsDefaultImageId)
//    Right(AutoClusterConfiguration(BcsJob.BcsDefaultResourceType, instanceType, imageId, None, None, None))
//  }

  private  def jobResource: Map[String, String] = {
    val cpu = runtime.cpu.getOrElse(throw new Exception("Invalid cpu"))
    val memory = runtime.memory.getOrElse("Invalid memory")
    lazyResource ++ Map(BcsJob.BcsResourceKeyCpu -> cpu.toString()) ++ Map(BcsJob.BcsResourceKeyMemory -> memory.toString)
  }

//  private[bcs] def taskDesc: TaskDescription = {
//    lazyTask.setParameters(params)
//    lazyTask.setInstanceCount(1)
//
//    runtime.timeout foreach {timeout => lazyTask.setTimeout(timeout.toLong)}
//
//    val cluster = runtime.cluster getOrElse getServerlessClusterConfiguration(runtime)
//    cluster.fold(handleClusterId, handleAutoCluster)
//
//    val mnts = new Mounts
//    mounts foreach  {
//      case input: BcsInputMount =>
//        mnts.addEntries(input.toBcsMountEntry)
//      case output: BcsOutputMount =>
//        var srcStr = BcsMount.toString(output.src)
//        if (BcsMount.toString(output.dest).endsWith("/") && !srcStr.endsWith("/")) {
//          srcStr += "/"
//        }
//        lazyTask.addOutputMapping(srcStr, BcsMount.toString(output.dest))
//    }
//
//    lazyTask.setMounts(mnts)
//
//    lazyTask
//  }

//  private def handleAutoCluster(config: AutoClusterConfiguration): Unit = {
//    val autoCluster = new AutoCluster
//    autoCluster.setImageId(runtime.imageId.getOrElse(config.imageId))
//    autoCluster.setInstanceType(config.instanceType)
//    autoCluster.setResourceType(config.resourceType)
//
//    config.spotStrategy foreach {strategy => autoCluster.setSpotStrategy(strategy)}
//    config.spotPriceLimit foreach {priceLimit => autoCluster.setSpotPriceLimit(priceLimit)}
//    config.clusterId foreach {clusterId => autoCluster.setClusterId(clusterId)}
//    runtime.reserveOnFail foreach {reserve => autoCluster.setReserveOnFail(reserve)}
//    val userData = runtime.userData map {datas => Map(datas map {data => data.key -> data.value}: _*)}
//    userData foreach {datas => autoCluster.setUserData(datas.asJava)}
//
//    configs foreach (bcsConfigs => autoCluster.setConfigs(bcsConfigs))
//    runtime.isv foreach(isv => autoCluster.setDependencyIsvService(isv))
//
//    lazyTask.setAutoCluster(autoCluster)
//  }

//  private def handleClusterId(clusterId: String): Unit = lazyTask.setClusterId(clusterId)

  private def handleManagedJobQueue(clusterId: String): Unit = {
    val _: Runtimes = lazyRuntime.setJobQueue(clusterId)
  }
  private def handleServerlessJob(config: AutoClusterConfiguration): Unit = {}
}
