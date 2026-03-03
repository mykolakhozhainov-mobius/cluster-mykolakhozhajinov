/*
 * Copyright 2022-2023, Mobius Software LTD. and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package org.restcomm.cluster;

import java.io.Externalizable;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.TransactionManager;

import org.restcomm.cache.infinispan.InfinispanCacheFactory;
import org.restcomm.cluster.serializers.Serializer;

/**
 * 
 * @author yulian.oifa
 * 
 */
public class RestcommClusterFactory {
	private IDGenerator<?> idGenerator;    
	private ConcurrentHashMap<String, RestcommCluster> clustersMap = new ConcurrentHashMap<String, RestcommCluster>();
	
	private RestcommCacheFactory cacheFactory;
	private CacheDataExecutorService service;
	private Serializer serializer;
	
	public RestcommClusterFactory(String clusterName, String clusterType,SerializationType serializationType, TransactionManager transactionManager, ClassLoader classLoader, CacheExecutorConfiguration executorConfiguration,Integer aquireTimeout,List<String> hosts,Integer port,List<String> zkHosts,Integer zkPort,Integer maxBufferSize, Boolean isAsync, Boolean paritioned, Integer copies, Boolean logStats, Long maxIdleMs) {
		this.idGenerator=new UUIDGenerator();
		
		this.service=new CacheDataExecutorService(executorConfiguration, idGenerator, classLoader);
		this.serializer=SerializerFactory.createSerializer(serializationType, classLoader, service);		
		switch(clusterType) {
			case "INFINISPAN_REPLICATED":
				cacheFactory=new InfinispanCacheFactory(clusterName, transactionManager, serializer, idGenerator, classLoader,service,aquireTimeout, isAsync, true, paritioned, copies, logStats, maxIdleMs);
				break;
			case "INFINISPAN_LOCAL":
			default:
				cacheFactory=new InfinispanCacheFactory(clusterName, transactionManager, serializer, idGenerator, classLoader,service,aquireTimeout, isAsync, false, paritioned, copies, logStats, maxIdleMs);
				break;			
		}
	}

	public RestcommCluster getCluster(String name,Boolean isTree) {
		RestcommCluster cluster = clustersMap.get(name);
		if (cluster == null) {		
			cluster = cacheFactory.getCluster(name,isTree);
			RestcommCluster oldValue = clustersMap.putIfAbsent(name, cluster);
			if (oldValue != null)
				cluster = oldValue;						
		}
		return cluster;
	}

	public void stopCluster(String name) {
		RestcommCluster cluster = clustersMap.remove(name);
		if (cluster != null) {
			cluster.stopCluster();	
			cacheFactory.removeCluster(name);
		}
	}

	public IDGenerator<?> getIDGenerator() {
		return idGenerator;
	}
	
	public void prepareThreadForCache() {
		service.initMirror();
	}
	
	public void cleanupThreadForCache() {
		service.removeMirror();
	}
	
	public void stop() {
		Iterator<Entry<String, RestcommCluster>> iterator=clustersMap.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<String, RestcommCluster> curr=iterator.next();
			RestcommCluster cluster=clustersMap.remove(curr.getKey());
			if(cluster!=null) {
			    cluster.stopCluster();
			    cacheFactory.removeCluster(curr.getKey());
			}
		}
		
		if(this.cacheFactory!=null) {
			this.cacheFactory.stop();
			this.cacheFactory=null;
		}
	}
	
	public void registerKnownClass(int id, Class<? extends Externalizable> knownClass) {
		if(serializer!=null)
			serializer.registerKnownClass(id, knownClass);
		
		cacheFactory.registerKnownClass(id, knownClass);
	}
}