import unittest
import sys
import os

# Add src to path to import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from workers.Worker import Worker


class TestWorker(unittest.TestCase):
    def test_worker_is_abstract(self):
        """Test that Worker is an abstract base class."""
        with self.assertRaises(TypeError):
            Worker()

    def test_run_method_not_implemented(self):
        """Test that run method raises NotImplementedError when called on incomplete implementation."""
        class TestWorkerImplementation(Worker):
            def health_check(self):
                return super().health_check()
            
            async def listen_task(self):
                return await super().listen_task()
        
        # This will fail at instantiation since run is still abstract
        with self.assertRaises(TypeError):
            TestWorkerImplementation()

    def test_health_check_method_not_implemented(self):
        """Test that health_check method raises NotImplementedError when called on incomplete implementation."""
        class TestWorkerImplementation(Worker):
            async def run(self):
                return await super().run()
            
            async def listen_task(self):
                return await super().listen_task()
        
        # This will fail at instantiation since health_check is still abstract
        with self.assertRaises(TypeError):
            TestWorkerImplementation()

    def test_listen_task_method_not_implemented(self):
        """Test that listen_task method raises NotImplementedError when called on incomplete implementation."""
        class TestWorkerImplementation(Worker):
            async def run(self):
                return await super().run()
            
            def health_check(self):
                return super().health_check()
        
        # This will fail at instantiation since listen_task is still abstract
        with self.assertRaises(TypeError):
            TestWorkerImplementation()

    def test_worker_interface_methods_exist(self):
        """Test that all required abstract methods exist in the Worker interface."""
        # Verify the abstract methods are defined
        self.assertTrue(hasattr(Worker, 'run'))
        self.assertTrue(hasattr(Worker, 'health_check'))
        self.assertTrue(hasattr(Worker, 'listen_task'))
        
        # Verify they are abstract
        self.assertTrue(getattr(Worker.run, '__isabstractmethod__', False))
        self.assertTrue(getattr(Worker.health_check, '__isabstractmethod__', False))
        self.assertTrue(getattr(Worker.listen_task, '__isabstractmethod__', False))

    def test_complete_worker_implementation(self):
        """Test that a complete implementation of Worker can be instantiated."""
        class CompleteWorkerImplementation(Worker):
            async def run(self):
                return "run executed"
            
            def health_check(self):
                return "health check executed"
            
            async def listen_task(self):
                return "listen task executed"
        
        # Should not raise any errors
        worker = CompleteWorkerImplementation()
        self.assertIsInstance(worker, Worker)
        self.assertIsInstance(worker, CompleteWorkerImplementation)
        
        # Test methods can be called
        self.assertEqual(worker.health_check(), "health check executed")
        
        # Test async methods
        import asyncio
        self.assertEqual(asyncio.run(worker.run()), "run executed")
        self.assertEqual(asyncio.run(worker.listen_task()), "listen task executed")

    def test_abstract_method_call_raises_not_implemented(self):
        """Test that calling abstract methods directly raises NotImplementedError."""
        class PartialWorkerImplementation(Worker):
            async def run(self):
                return "run executed"
            
            def health_check(self):
                return "health check executed"
            
            async def listen_task(self):
                # Call super to test NotImplementedError
                return await super().listen_task()
        
        worker = PartialWorkerImplementation()
        
        # This should raise NotImplementedError when called
        import asyncio
        with self.assertRaises(NotImplementedError):
            asyncio.run(worker.listen_task())


if __name__ == '__main__':
    unittest.main()