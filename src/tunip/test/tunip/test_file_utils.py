import unittest
import urllib.parse

from pathlib import Path

import tunip.file_utils as fu

from tunip.config import Config
from tunip.constants import SPACE
from tunip.file_utils import HttpBasedWebHdfsFileHandler
from tunip.service_config import get_service_config


class FileUtilsTest(unittest.TestCase):

    def setUp(self):
        config = Config(
            Path(__file__).parent.parent.parent.parent.parent / "experiments" / "application.json"
        )
        self.hdfs_handler = fu.services.get("HDFS", config=config)
        self.local_handler = fu.services.get("LOCAL", config=config)

    def test_hdfs_handler_list(self):
        lake_dir_list = self.hdfs_handler.list_dir("/user/nauts/lake/tagged_entity/serp.small_span")
        assert len(lake_dir_list) > 0

    def test_local_handler_list(self):
        dirs = self.local_handler.list_dir(Path(__file__).parent.parent.parent.parent.parent)
        assert len(dirs) > 0

    def test_loads_pickle_keyword_trie(self):
        trie_path = '/user/nauts/warehouse/entity_trie/wiki/wiki.small_span/20220121_114303_960817/entity_trie.pkl'
        assert self.hdfs_handler.exists(trie_path)
        if self.hdfs_handler.exists(trie_path):
            trie = self.hdfs_handler.loads_pickle(trie_path)
            assert '수호성인' in trie

    def test_loads_pickle_entity_trie(self):
        trie_path = '/user/nauts/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360/entity_trie.pkl'
        assert self.hdfs_handler.exists(trie_path)
        if self.hdfs_handler.exists(trie_path):
            trie = self.hdfs_handler.loads_pickle(trie_path)
            assert '히로시마 고속 교통'.replace(' ', SPACE) in trie

    def test_loads_and_dumps_pickle_entity_trie(self):
        trie_path = '/user/nauts/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360/entity_trie.pkl'
        assert self.hdfs_handler.exists(trie_path)
        if self.hdfs_handler.exists(trie_path):
            trie = self.hdfs_handler.loads_pickle(trie_path)

            new_trie_path = '/user/nauts/test/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360/entity_trie.pkl'
            self.hdfs_handler.mkdirs('/user/nauts/test/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360')
            self.hdfs_handler.dumps_pickle(new_trie_path, trie)
            new_trie = self.hdfs_handler.loads_pickle(new_trie_path)

            assert '히로시마 고속 교통'.replace(' ', SPACE) in new_trie

            self.hdfs_handler.client.delete('/user/nauts/test/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360', recursive=True, skip_trash=True)

    def test_http_webhdfs(self):
        service_config = get_service_config(force_service_level='dev')
        model_name = 'monologg/koelectra-small-v3-discriminator'
        config_path = f"/user/nauts/mart/plm/models/{urllib.parse.quote_plus(urllib.parse.quote_plus(model_name))}/config.json"

        webhdfs_handle = HttpBasedWebHdfsFileHandler(service_config)
        f = webhdfs_handle.open(config_path, mode='rb')
        assert f.content_length > 0

    def test_download_file_config_of_http_webhdfs(self):
        service_config = get_service_config(force_service_level='dev')
        model_name = 'monologg/koelectra-small-v3-discriminator'
        config_path = f"/user/nauts/mart/plm/models/{urllib.parse.quote_plus(model_name)}/config.json"

        webhdfs_handle = HttpBasedWebHdfsFileHandler(service_config)
        # f = webhdfs_handle.open(config_path, mode='rb')
        webhdfs_handle.download_file(
            config_path,
            f"/user/{service_config.username}/test_config.json",
            overwrite=True,
            read_mode='r',
            write_mode='w'
        )

    def test_download_binary_of_http_webhdfs(self):
        service_config = get_service_config(force_service_level='dev')
        model_name = 'monologg/koelectra-small-v3-discriminator'
        model_path = f"/user/nauts/mart/plm/models/{urllib.parse.quote_plus(model_name)}/torchscript/model.pt"

        webhdfs_handle = HttpBasedWebHdfsFileHandler(service_config)
        try:
            webhdfs_handle.download_file(
                model_path,
                f"/user/{service_config.username}/mymodel.pt",
                overwrite=True,
                read_mode='rb',
                write_mode='wb'
            )
        except Exception:
            self.fail("webhdfs_handle.download() raised ExceptionType unexpectedly!")
        
    def test_download_directory_of_http_webhdfs(self):
        service_config = get_service_config(force_service_level='dev')
        model_name = 'monologg/koelectra-small-v3-discriminator'
        dir_path = f"/user/nauts/mart/plm/models/{urllib.parse.quote_plus(model_name)}"

        webhdfs_handle = HttpBasedWebHdfsFileHandler(service_config)
        # paths = webhdfs_handle.client.walk(dir_path)
        # print(next(paths))

        # NOT WORKING with the same path
        # paths = webhdfs_handle.client.download(dir_path, 'test_out')
        # hdfs.util.HdfsError: File does not exist: /user/nauts/mart/plm/models/monologg/koelectra-small-v3-discriminator/config.json
        # at org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:86)
        # at org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:76)
        # at org.apache.hadoop.hdfs.server.namenode.FSDirStatAndListingOp.getBlockLocations(FSDirStatAndListingOp.java:158)
        # at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocations(FSNamesystem.java:1931)
        # at org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.getBlockLocations(NameNodeRpcServer.java:738)
        # at org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB.getBlockLocations(ClientNamenodeProtocolServerSideTranslatorPB.java:426)
        # at org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos$ClientNamenodeProtocol$2.callBlockingMethod(ClientNamenodeProtocolProtos.java)
        # at org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine.java:524)
        # at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:1025)
        # at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:876)
        # at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:822)
        # at java.security.AccessController.doPrivileged(Native Method)
        # at javax.security.auth.Subject.doAs(Subject.java:422)
        # at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1730)
        # at org.apache.hadoop.ipc.Server$Handler.run(Server.java:2682)

        try:
            webhdfs_handle.download(
                hdfs_path=dir_path,
                overwrite=True,
                read_mode='rb',
                write_mode='wb'
            )
        except Exception:
            self.fail("webhdfs_handle.download() raised ExceptionType unexpectedly!")
        
    def test_download_config_of_http_webhdfs(self):
        service_config = get_service_config(force_service_level='dev')
        model_name = 'monologg/koelectra-small-v3-discriminator'
        config_path = f"/user/nauts/mart/plm/models/{urllib.parse.quote_plus(model_name)}/config.json"

        webhdfs_handle = HttpBasedWebHdfsFileHandler(service_config)
        # f = webhdfs_handle.open(config_path, mode='rb')
        webhdfs_handle.download(
            config_path,
            # f"/user/{service_config.username}/test_config.json",
            overwrite=True,
            read_mode='r',
            write_mode='w'
        )
            
    def test_hdfs_handler_write(self):
        output_path = "/user/nauts/test1.txt"
        append = True if self.hdfs_handler.exists(output_path) else False
        self.hdfs_handler.write(output_path, "test", append=append)

        assert self.hdfs_handler.exists(output_path)
        
        self.hdfs_handler.client.delete(output_path)
        
    def test_local_handler_write(self):
        output_path = "test1.txt"
        
        append = True if self.local_handler.exists(Path(self.local_handler.local_path_builder.build(output_path))) else False
        self.local_handler.write(Path(output_path), "test\n", append=append)

        assert self.local_handler.exists(Path(self.local_handler.local_path_builder.build(output_path)))
