import java.rmi.Remote;
import java.rmi.RemoteException;

public interface HotelBooking extends Remote {
    // Book a room for a specific guest
    String bookRoom(String guestName, int roomNumber) throws RemoteException;
    
    // Cancel a booking for a specific guest
    String cancelBooking(String guestName) throws RemoteException;
    
    // Get current bookings (for demonstration)
    String listBookings() throws RemoteException;
}